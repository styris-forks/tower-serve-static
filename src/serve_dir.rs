use super::{AsyncReadBody, DEFAULT_CAPACITY};
use bytes::Bytes;
use http::{header, HeaderValue, Request, Response, StatusCode, Uri};
use http_body::Frame;
use http_body_util::{combinators::BoxBody, BodyExt, Empty};
use include_dir::{Dir, File};
use papaya::LocalGuard;
use percent_encoding::percent_decode;
use std::{
    convert::Infallible,
    future::Future,
    io,
    path::{Path, PathBuf},
    pin::Pin,
    task::{Context, Poll},
};
use tower_service::Service;
use xxhash_rust::xxh3::Xxh3Builder;

/// Service that serves files from a given directory and all its sub directories.
///
/// The `Content-Type` will be guessed from the file extension.
///
/// An empty response with status `404 Not Found` will be returned if:
///
/// - The file doesn't exist
/// - Any segment of the path contains `..`
/// - Any segment of the path contains a backslash
#[derive(Clone, Debug)]
pub struct ServeDir {
    dir: &'static Dir<'static>,
    cache: &'static papaya::HashMap<PathBuf, ServeEntry, Xxh3Builder>,
    append_index_html_on_directories: bool,
    redirect_not_found_to_index_html: bool,
    buf_chunk_size: usize,
    brotli: bool,
}

/// Represents an entry in the serve directory cache.
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(missing_docs)]
pub enum ServeEntry {
    File {
        data: &'static File<'static>,
        mime: HeaderValue,
        brotli: bool,
    },
    Dir,
}

impl ServeDir {}

impl ServeDir {
    /// Create a new [`ServeDir`].
    pub fn new(
        dir: &'static Dir<'static>,
        cache: &'static papaya::HashMap<PathBuf, ServeEntry, Xxh3Builder>,
    ) -> Self {
        Self {
            dir,
            cache,
            append_index_html_on_directories: true,
            redirect_not_found_to_index_html: false,
            buf_chunk_size: DEFAULT_CAPACITY,
            brotli: false,
        }
    }

    /// If the requested path is a directory append `index.html`.
    ///
    /// This is useful for static sites.
    ///
    /// Defaults to `true`.
    pub fn append_index_html_on_directories(mut self, append: bool) -> Self {
        self.append_index_html_on_directories = append;
        self
    }

    /// Redirect to `index.html` when a file is not found.
    ///
    /// This is useful for SPA applications.
    ///
    /// Defaults to `false`.
    pub fn redirect_not_found_to_index_html(mut self, redirect: bool) -> Self {
        self.redirect_not_found_to_index_html = redirect;
        self
    }

    /// Set a specific read buffer chunk size.
    ///
    /// The default capacity is 64kb.
    pub fn with_buf_chunk_size(mut self, chunk_size: usize) -> Self {
        self.buf_chunk_size = chunk_size;
        self
    }

    /// Informs the service that it should also look for a precompressed brotli
    /// version of _any_ file in the directory.
    ///
    /// Assuming the `dir` directory is being served and `dir/foo.txt` is requested,
    /// a client with an `Accept-Encoding` header that allows the brotli encoding
    /// will receive the file `dir/foo.txt.br` instead of `dir/foo.txt`.
    /// If the precompressed file is not available, or the client doesn't support it,
    /// the uncompressed version will be served instead (if available).
    pub fn precompressed_br(mut self) -> Self {
        self.brotli = true;
        self
    }
}

impl<ReqBody> Service<Request<ReqBody>> for ServeDir {
    type Response = Response<ResponseBody>;
    type Error = Infallible;
    type Future = ResponseFuture;

    #[inline]
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        // build and validate the path
        let path = req.uri().path();
        let path = path.trim_start_matches('/');

        let path_decoded = if let Ok(decoded_utf8) = percent_decode(path.as_ref()).decode_utf8() {
            decoded_utf8
        } else {
            return ResponseFuture {
                inner: Some(Inner::Invalid),
            };
        };

        let mut full_path = PathBuf::new();
        for seg in path_decoded.split('/') {
            if seg.starts_with("..") || seg.contains('\\') {
                return ResponseFuture {
                    inner: Some(Inner::Invalid),
                };
            }
            full_path.push(seg);
        }

        if !req.uri().path().ends_with('/') {
            if is_dir(self.dir, &mut self.cache, &full_path) {
                let location =
                    HeaderValue::from_str(&append_slash_on_path(req.uri().clone()).to_string())
                        .unwrap();
                return ResponseFuture {
                    inner: Some(Inner::Redirect(location)),
                };
            }
        } else if is_dir(self.dir, &mut self.cache, &full_path) {
            if self.append_index_html_on_directories {
                full_path.push("index.html");
            } else {
                return ResponseFuture {
                    inner: Some(Inner::NotFound),
                };
            }
        }
        let Some((file, brotli, mime)) = resolve_file(
            self.dir,
            &mut self.cache,
            &mut full_path,
            self.brotli && accepts_brotli(req.headers()),
            self.redirect_not_found_to_index_html,
        ) else {
            return ResponseFuture {
                inner: Some(Inner::NotFound),
            };
        };

        #[cfg(feature = "metadata")]
        if super::unmodified_since_request_condition(file, &req) {
            return ResponseFuture {
                inner: Some(Inner::NotModified),
            };
        }

        ResponseFuture {
            inner: Some(Inner::File(file, mime, brotli, self.buf_chunk_size)),
        }
    }
}

fn is_dir(
    dir: &Dir<'static>,
    cache: &papaya::HashMap<PathBuf, ServeEntry, Xxh3Builder>,
    path: &Path,
) -> bool {
    if path.as_os_str() == std::ffi::OsStr::new("") {
        return true;
    }

    if let Some(entry) = cache.pin().get(path) {
        return entry == &ServeEntry::Dir;
    }

    let result = dir.get_dir(path).is_some();
    if result {
        cache.pin().insert(path.to_path_buf(), ServeEntry::Dir);
    }
    result
}

fn append_slash_on_path(uri: Uri) -> Uri {
    let http::uri::Parts {
        scheme,
        authority,
        path_and_query,
        ..
    } = uri.into_parts();

    let mut builder = Uri::builder();
    if let Some(scheme) = scheme {
        builder = builder.scheme(scheme);
    }
    if let Some(authority) = authority {
        builder = builder.authority(authority);
    }
    if let Some(path_and_query) = path_and_query {
        if let Some(query) = path_and_query.query() {
            builder = builder.path_and_query(format!("{}/?{}", path_and_query.path(), query));
        } else {
            builder = builder.path_and_query(format!("{}/", path_and_query.path()));
        }
    } else {
        builder = builder.path_and_query("/");
    }

    builder.build().unwrap()
}

fn cached_get_file<'a>(
    dir: &Dir<'static>,
    cache: &'a papaya::HashMapRef<'_, PathBuf, ServeEntry, Xxh3Builder, LocalGuard<'_>>,
    path: &mut PathBuf,
    brotli: bool,
) -> Option<&'a ServeEntry> {
    if let Some(entry) = cache.get(path) {
        return Some(entry);
    }

    let mime = mime_guess::from_path(&path)
        .first_raw()
        .map(HeaderValue::from_static)
        .unwrap_or_else(|| HeaderValue::from_str(mime::APPLICATION_OCTET_STREAM.as_ref()).unwrap());

    if brotli {
        path.add_extension("br");
    }

    let file = match dir.get_file(&path) {
        Some(f) => f,
        None => {
            if brotli {
                path.set_extension("");
            } else {
                return None;
            }
            match dir.get_file(&path) {
                Some(file) => {
                    cache.insert(
                        path.clone(),
                        ServeEntry::File {
                            data: file,
                            mime,
                            brotli: false,
                        },
                    );
                    return Some(cache.get(path).unwrap());
                }
                None => return None,
            }
        }
    };

    if brotli {
        path.set_extension("");
    }
    cache.insert(
        path.clone(),
        ServeEntry::File {
            data: file,
            mime,
            brotli: brotli,
        },
    );
    Some(cache.get(path).unwrap())
}

fn resolve_file(
    dir: &Dir<'static>,
    cache: &papaya::HashMap<PathBuf, ServeEntry, Xxh3Builder>,
    path: &mut PathBuf,
    brotli: bool,
    redirect_not_found_to_index_html: bool,
) -> Option<(&'static File<'static>, bool, HeaderValue)> {
    if !brotli {
        let cache = cache.pin();
        let Some(entry) = cached_get_file(dir, &cache, path, false) else {
            return None;
        };

        match entry {
            ServeEntry::File {
                data,
                mime,
                brotli: cached_brotli,
            } => {
                if !cached_brotli {
                    return Some((*data, false, mime.clone()));
                } else {
                    // Slow path, check for uncompressed version
                    if let Some(file) = dir.get_file(&path) {
                        return Some((file, false, mime.clone()));
                    };

                    if !redirect_not_found_to_index_html {
                        return None;
                    }

                    if let Some(file) = dir.get_file(&"index.html") {
                        return Some((
                            file,
                            false,
                            HeaderValue::from_str(mime::HTML.as_ref()).unwrap(),
                        ));
                    };
                }
                return None;
            }
            ServeEntry::Dir => return None,
        }
    }

    let cache = cache.pin();
    if let Some(entry) = cached_get_file(dir, &cache, path, true) {
        return match entry {
            ServeEntry::File { data, mime, brotli } => Some((*data, *brotli, mime.clone())),
            ServeEntry::Dir => None,
        };
    };

    if !redirect_not_found_to_index_html {
        return None;
    };

    if let Some(entry) = cached_get_file(dir, &cache, &mut PathBuf::from("index.html"), true) {
        return match entry {
            ServeEntry::File { data, mime, brotli } => Some((*data, *brotli, mime.clone())),
            ServeEntry::Dir => None,
        };
    };

    None
}
enum Inner {
    File(&'static File<'static>, HeaderValue, bool, usize),
    Redirect(HeaderValue),
    NotFound,
    Invalid,
    #[cfg(feature = "metadata")]
    NotModified,
}

/// Response future of [`ServeDir`].
pub struct ResponseFuture {
    inner: Option<Inner>,
}

impl Future for ResponseFuture {
    type Output = Result<Response<ResponseBody>, Infallible>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.inner.take().unwrap() {
            Inner::File(file, mime, brotli, chunk_size) => {
                let body = AsyncReadBody::with_capacity(file.contents(), chunk_size).boxed();
                let body = ResponseBody(body);

                let mut res = Response::new(body);
                res.headers_mut().insert(header::CONTENT_TYPE, mime);
                if brotli {
                    res.headers_mut()
                        .insert(header::CONTENT_ENCODING, HeaderValue::from_static("br"));
                }

                #[cfg(feature = "metadata")]
                if let Some(metadata) = file.metadata() {
                    let modified = httpdate::HttpDate::from(metadata.modified()).to_string();
                    let value = HeaderValue::from_str(&modified).expect("SystemTime format");
                    res.headers_mut().insert(header::LAST_MODIFIED, value);
                }

                Poll::Ready(Ok(res))
            }
            Inner::Redirect(location) => {
                let res = Response::builder()
                    .header(http::header::LOCATION, location)
                    .status(StatusCode::TEMPORARY_REDIRECT)
                    .body(empty_body())
                    .unwrap();

                Poll::Ready(Ok(res))
            }
            Inner::NotFound | Inner::Invalid => {
                let res = Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(empty_body())
                    .unwrap();

                Poll::Ready(Ok(res))
            }
            #[cfg(feature = "metadata")]
            Inner::NotModified => {
                let res = Response::builder()
                    .status(StatusCode::NOT_MODIFIED)
                    .body(empty_body())
                    .unwrap();

                Poll::Ready(Ok(res))
            }
        }
    }
}

fn empty_body() -> ResponseBody {
    let body = Empty::new().map_err(|err| match err {}).boxed();
    ResponseBody(body)
}

opaque_body! {
    /// Response body for [`ServeDir`].
    pub type ResponseBody = BoxBody<Bytes, io::Error>;
}

fn accepts_brotli<'a>(headers: &'a http::HeaderMap) -> bool {
    headers
        .get_all(http::header::ACCEPT_ENCODING)
        .iter()
        .filter_map(|hval| hval.to_str().ok())
        .flat_map(|s| s.split(','))
        .any(move |v| {
            let mut v = v.splitn(2, ';');

            v.next().unwrap().trim().eq_ignore_ascii_case("br")
        })
}

#[cfg(test)]
mod tests {
    use std::sync::OnceLock;

    #[allow(unused_imports)]
    use super::*;
    use http::{Request, StatusCode};
    use http_body::Body as HttpBody;
    use include_dir::include_dir;
    use tower::ServiceExt;

    static CLIENT_SERVE_CACHE: OnceLock<papaya::HashMap<PathBuf, ServeEntry, Xxh3Builder>> =
        OnceLock::new();
    static ASSETS_DIR: Dir<'static> = include_dir!("$CARGO_MANIFEST_DIR/tests/assets");

    #[tokio::test]
    async fn basic() {
        let svc = ServeDir::new(
            &ASSETS_DIR,
            CLIENT_SERVE_CACHE.get_or_init(|| papaya::HashMap::with_hasher(Xxh3Builder::default())),
        );

        let req = Request::builder()
            .uri("/text.txt")
            .body(http_body_util::Empty::<Bytes>::new())
            .unwrap();
        let res = svc.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(res.headers()["content-type"], "text/plain");
        #[cfg(not(feature = "metadata"))]
        {
            assert!(!res.headers().contains_key("last-modified"));
        }
        #[cfg(feature = "metadata")]
        {
            assert!(res.headers().contains_key("last-modified"));
        }

        let body = body_into_text(res.into_body()).await;

        let contents = std::fs::read_to_string("./tests/assets/text.txt").unwrap();
        assert_eq!(body, contents);
    }

    #[cfg(feature = "metadata")]
    #[tokio::test]
    async fn with_if_modified_since() {
        let svc = ServeDir::new(
            &ASSETS_DIR,
            CLIENT_SERVE_CACHE.get_or_init(|| papaya::HashMap::with_hasher(Xxh3Builder::default())),
        );

        let modified: httpdate::HttpDate = ASSETS_DIR
            .get_file("text.txt")
            .unwrap()
            .metadata()
            .unwrap()
            .modified()
            .into();

        let req = Request::builder()
            .uri("/text.txt")
            .header(
                header::IF_MODIFIED_SINCE,
                HeaderValue::from_str(&modified.to_string()).unwrap(),
            )
            .body(http_body_util::Empty::<Bytes>::new())
            .unwrap();
        let res = svc.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::NOT_MODIFIED);
        assert!(!res.headers().contains_key("content-type"));
        assert!(!res.headers().contains_key("last-modified"));
        assert!(body_into_text(res.into_body()).await.is_empty());
    }

    #[tokio::test]
    async fn with_custom_chunk_size() {
        let svc = ServeDir::new(
            &ASSETS_DIR,
            CLIENT_SERVE_CACHE.get_or_init(|| papaya::HashMap::with_hasher(Xxh3Builder::default())),
        )
        .with_buf_chunk_size(1024 * 32);

        let req = Request::builder()
            .uri("/text.txt")
            .body(http_body_util::Empty::<Bytes>::new())
            .unwrap();
        let res = svc.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(res.headers()["content-type"], "text/plain");

        let body = body_into_text(res.into_body()).await;

        let contents = std::fs::read_to_string("./tests/assets/text.txt").unwrap();
        assert_eq!(body, contents);
    }

    #[tokio::test]
    async fn access_to_sub_dirs() {
        let svc = ServeDir::new(
            &ASSETS_DIR,
            CLIENT_SERVE_CACHE.get_or_init(|| papaya::HashMap::with_hasher(Xxh3Builder::default())),
        );

        let req = Request::builder()
            .uri("/subfolder/data.json")
            .body(http_body_util::Empty::<Bytes>::new())
            .unwrap();
        let res = svc.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(res.headers()["content-type"], "application/json");

        let body = body_into_text(res.into_body()).await;

        let contents = std::fs::read_to_string("./tests/assets/subfolder/data.json").unwrap();
        assert_eq!(body, contents);
    }

    #[tokio::test]
    async fn not_found() {
        let svc = ServeDir::new(
            &ASSETS_DIR,
            CLIENT_SERVE_CACHE.get_or_init(|| papaya::HashMap::with_hasher(Xxh3Builder::default())),
        );

        let req = Request::builder()
            .uri("/not-found")
            .body(http_body_util::Empty::<Bytes>::new())
            .unwrap();
        let res = svc.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        assert!(res.headers().get(header::CONTENT_TYPE).is_none());

        let body = body_into_text(res.into_body()).await;
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn redirect_to_trailing_slash_on_dir() {
        let svc = ServeDir::new(
            &ASSETS_DIR,
            CLIENT_SERVE_CACHE.get_or_init(|| papaya::HashMap::with_hasher(Xxh3Builder::default())),
        );

        let req = Request::builder()
            .uri("/subfolder")
            .body(http_body_util::Empty::<Bytes>::new())
            .unwrap();
        let res = svc.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::TEMPORARY_REDIRECT);

        let location = &res.headers()[http::header::LOCATION];
        assert_eq!(location, "/subfolder/");
    }

    #[tokio::test]
    async fn empty_directory_without_index() {
        let svc = ServeDir::new(
            &ASSETS_DIR,
            CLIENT_SERVE_CACHE.get_or_init(|| papaya::HashMap::with_hasher(Xxh3Builder::default())),
        )
        .append_index_html_on_directories(false);

        let req = Request::new(http_body_util::Empty::<Bytes>::new());
        let res = svc.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        assert!(res.headers().get(header::CONTENT_TYPE).is_none());

        let body = body_into_text(res.into_body()).await;
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn root_path_with_index() {
        let svc = ServeDir::new(
            &ASSETS_DIR,
            CLIENT_SERVE_CACHE.get_or_init(|| papaya::HashMap::with_hasher(Xxh3Builder::default())),
        );

        let req = Request::builder()
            .uri("/")
            .body(http_body_util::Empty::<Bytes>::new())
            .unwrap();
        let res = svc.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(res.headers()["content-type"], "text/html");

        let body = body_into_text(res.into_body()).await;

        let contents = std::fs::read_to_string("./tests/assets/index.html").unwrap();
        assert_eq!(body, contents);
    }

    async fn body_into_text<B>(body: B) -> String
    where
        B: HttpBody<Data = bytes::Bytes> + Unpin,
        B::Error: std::fmt::Debug,
    {
        let bytes = body.collect().await.unwrap().to_bytes(); //.await.unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn access_cjk_percent_encoded_uri_path() {
        let svc = ServeDir::new(
            &ASSETS_DIR,
            CLIENT_SERVE_CACHE.get_or_init(|| papaya::HashMap::with_hasher(Xxh3Builder::default())),
        );

        let req = Request::builder()
            // percent encoding present of 你好世界.txt
            .uri("/%E4%BD%A0%E5%A5%BD%E4%B8%96%E7%95%8C.txt")
            .body(http_body_util::Empty::<Bytes>::new())
            .unwrap();
        let res = svc.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(res.headers()["content-type"], "text/plain");
    }

    #[tokio::test]
    async fn access_space_percent_encoded_uri_path() {
        let svc = ServeDir::new(
            &ASSETS_DIR,
            CLIENT_SERVE_CACHE.get_or_init(|| papaya::HashMap::with_hasher(Xxh3Builder::default())),
        );

        let req = Request::builder()
            // percent encoding present of "filename with space.txt"
            .uri("/filename%20with%20space.txt")
            .body(http_body_util::Empty::<Bytes>::new())
            .unwrap();
        let res = svc.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(res.headers()["content-type"], "text/plain");
    }
}
