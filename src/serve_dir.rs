use super::{AsyncReadBody, DEFAULT_CAPACITY};
use bytes::Bytes;
use http::{header, HeaderName, HeaderValue, Request, Response, StatusCode, Uri};
use http_body::Frame;
use http_body_util::{combinators::BoxBody, BodyExt, Empty};
use include_dir::{Dir, File};
use papaya::LocalGuard;
use percent_encoding::percent_decode;
use std::{
    convert::Infallible,
    ffi::OsString,
    future::Future,
    io,
    path::{Path, PathBuf},
    pin::Pin,
    task::{Context, Poll},
};
use tower_service::Service;
use xxhash_rust::xxh3::Xxh3Builder;

/// `Cache-Control` for content-addressed (fingerprinted) assets: cacheable forever and
/// never revalidated, since a changed file gets a new name.
const IMMUTABLE_CACHE_CONTROL: &str = "public, max-age=31536000, immutable";

/// Per-path (or default) response settings applied to served files.
///
/// Settings are looked up by the file's *logical* path (the path the client requested,
/// without any `.br` precompression suffix). Use the builder methods on [`ServeDir`]
/// ([`ServeDir::cache_control`], [`ServeDir::header`], and their `default_*` variants)
/// to populate the map rather than constructing this directly; it is `pub` only so the
/// map type can be named.
#[derive(Clone, Debug, Default)]
pub struct ServeSettings {
    /// `Cache-Control` header value to send for matching responses.
    pub cache_control: Option<HeaderValue>,
    /// Additional response headers to send (e.g. `Content-Security-Policy`,
    /// `X-Content-Type-Options`, `Cross-Origin-Resource-Policy`). Applied after the
    /// built-in `Content-Type` / `Content-Encoding`; a duplicate name replaces the prior
    /// value rather than appending.
    pub headers: Vec<(HeaderName, HeaderValue)>,
}

impl ServeSettings {
    fn set_cache_control(&mut self, value: HeaderValue) {
        self.cache_control = Some(value);
    }

    fn set_header(&mut self, name: HeaderName, value: HeaderValue) {
        self.headers.retain(|(existing, _)| existing != &name);
        self.headers.push((name, value));
    }

    /// Overlay `other` on top of `self`; fields/headers set in `other` win.
    fn overlay(&mut self, other: &ServeSettings) {
        if other.cache_control.is_some() {
            self.cache_control = other.cache_control.clone();
        }
        for (name, value) in &other.headers {
            self.set_header(name.clone(), value.clone());
        }
    }
}

/// The concurrent map of per-path [`ServeSettings`] passed to [`ServeDir::new`].
///
/// Backed by [`papaya`] so it can be read lock-free while serving and, if desired,
/// mutated at runtime. In the common case it is populated once up front through the
/// [`ServeDir`] builder methods and then only read.
pub type ServeSettingsMap = papaya::HashMap<PathBuf, ServeSettings, Xxh3Builder>;

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
    settings: &'static ServeSettingsMap,
    default_settings: ServeSettings,
    append_index_html_on_directories: bool,
    redirect_not_found_to_index_html: bool,
    buf_chunk_size: usize,
    brotli: bool,
}

impl ServeDir {
    /// Create a new [`ServeDir`].
    ///
    /// `settings` is a (typically empty) map populated through the builder methods; it is
    /// borrowed for `'static` so the service can be cheaply cloned across tasks.
    pub fn new(dir: &'static Dir<'static>, settings: &'static ServeSettingsMap) -> Self {
        Self {
            dir,
            settings,
            default_settings: ServeSettings::default(),
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

    /// Default `Cache-Control` value sent for every served file, unless a more specific
    /// per-path setting overrides it.
    pub fn default_cache_control(mut self, value: &str) -> Self {
        self.default_settings.set_cache_control(parse_header_value(value));
        self
    }

    /// Default extra response header sent for every served file (overridable per path).
    pub fn default_header(mut self, name: HeaderName, value: HeaderValue) -> Self {
        self.default_settings.set_header(name, value);
        self
    }

    /// Set the `Cache-Control` value for a specific file.
    ///
    /// If `path` names a directory in the embedded tree, the value is applied recursively
    /// to every file beneath it (each subpath is inserted into the settings map). An empty
    /// path (`""`) targets the whole served tree.
    pub fn cache_control(self, path: impl AsRef<Path>, value: &str) -> Self {
        let value = parse_header_value(value);
        self.apply(path.as_ref(), &|settings| settings.set_cache_control(value.clone()))
    }

    /// Set an extra response header for a specific file, or recursively for a directory
    /// (same path semantics as [`ServeDir::cache_control`]).
    pub fn header(self, path: impl AsRef<Path>, name: HeaderName, value: HeaderValue) -> Self {
        self.apply(path.as_ref(), &|settings| settings.set_header(name.clone(), value.clone()))
    }

    /// Send `Cache-Control: no-cache` for a file or directory (forces revalidation).
    pub fn no_cache(self, path: impl AsRef<Path>) -> Self {
        self.cache_control(path, "no-cache")
    }

    /// Default `Cache-Control: no-cache` for every served file (overridable per path).
    pub fn default_no_cache(self) -> Self {
        self.default_cache_control("no-cache")
    }

    /// Mark a file or directory as immutable
    /// (`public, max-age=31536000, immutable`) — for fingerprinted assets.
    pub fn immutable(self, path: impl AsRef<Path>) -> Self {
        self.cache_control(path, IMMUTABLE_CACHE_CONTROL)
    }

    /// Default immutable `Cache-Control` for every served file (overridable per path).
    pub fn default_immutable(self) -> Self {
        self.default_cache_control(IMMUTABLE_CACHE_CONTROL)
    }

    /// Send `Cache-Control: public, max-age=<seconds>` for a file or directory.
    pub fn max_age(self, path: impl AsRef<Path>, seconds: u64) -> Self {
        self.cache_control(path, &format!("public, max-age={seconds}"))
    }

    /// Default `Cache-Control: public, max-age=<seconds>` for every served file.
    pub fn default_max_age(self, seconds: u64) -> Self {
        self.default_cache_control(&format!("public, max-age={seconds}"))
    }

    /// Apply a mutation to the settings of `path`, expanding directories recursively.
    fn apply(self, path: &Path, mutate: &dyn Fn(&mut ServeSettings)) -> Self {
        let map = self.settings.pin();
        if let Some(dir) = dir_at(self.dir, path) {
            for_each_file(dir, &mut |file| {
                // The logical (non-`.br`) path is what gets looked up at request time, so
                // skip precompressed siblings — their settings would never be consulted.
                if !is_brotli(file.path()) {
                    upsert(&map, file.path(), mutate);
                }
            });
        } else {
            upsert(&map, path, mutate);
        }
        drop(map);
        self
    }

    /// Resolve the effective settings for a logical path: defaults overlaid with any
    /// per-path entry.
    fn effective_settings(&self, logical: &Path) -> ServeSettings {
        let mut settings = self.default_settings.clone();
        if let Some(specific) = self.settings.pin().get(logical) {
            settings.overlay(specific);
        }
        settings
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
            if is_dir(self.dir, &full_path) {
                let location =
                    HeaderValue::from_str(&append_slash_on_path(req.uri().clone()).to_string())
                        .unwrap();
                return ResponseFuture {
                    inner: Some(Inner::Redirect(location)),
                };
            }
        } else if is_dir(self.dir, &full_path) {
            if self.append_index_html_on_directories {
                full_path.push("index.html");
            } else {
                return ResponseFuture {
                    inner: Some(Inner::NotFound),
                };
            }
        }

        let Some(resolved) = resolve_file(
            self.dir,
            &full_path,
            self.brotli && accepts_brotli(req.headers()),
            self.redirect_not_found_to_index_html,
        ) else {
            return ResponseFuture {
                inner: Some(Inner::NotFound),
            };
        };

        #[cfg(feature = "metadata")]
        if super::unmodified_since_request_condition(resolved.file, &req) {
            return ResponseFuture {
                inner: Some(Inner::NotModified),
            };
        }

        let settings = self.effective_settings(&resolved.logical);

        ResponseFuture {
            inner: Some(Inner::File {
                file: resolved.file,
                mime: resolved.mime,
                brotli: resolved.brotli,
                // The resource is content-negotiated whenever brotli is enabled, even if
                // this particular client didn't accept it, so the response varies.
                vary_accept_encoding: self.brotli,
                chunk_size: self.buf_chunk_size,
                cache_control: settings.cache_control,
                headers: settings.headers,
            }),
        }
    }
}

fn is_dir(dir: &Dir<'static>, path: &Path) -> bool {
    path.as_os_str().is_empty() || dir.get_dir(path).is_some()
}

/// Returns the embedded directory at `path`, treating an empty path as the root.
fn dir_at(root: &'static Dir<'static>, path: &Path) -> Option<&'static Dir<'static>> {
    if path.as_os_str().is_empty() {
        Some(root)
    } else {
        root.get_dir(path)
    }
}

/// Visit every file under `dir` recursively.
fn for_each_file(dir: &'static Dir<'static>, visit: &mut dyn FnMut(&'static File<'static>)) {
    for file in dir.files() {
        visit(file);
    }
    for sub in dir.dirs() {
        for_each_file(sub, visit);
    }
}

/// Insert/merge a settings mutation for `key`.
fn upsert(
    map: &papaya::HashMapRef<'_, PathBuf, ServeSettings, Xxh3Builder, LocalGuard<'_>>,
    key: &Path,
    mutate: &dyn Fn(&mut ServeSettings),
) {
    let mut entry = map.get(key).cloned().unwrap_or_default();
    mutate(&mut entry);
    map.insert(key.to_path_buf(), entry);
}

fn is_brotli(path: &Path) -> bool {
    path.extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("br"))
}

fn parse_header_value(value: &str) -> HeaderValue {
    HeaderValue::from_str(value).expect("ServeDir: invalid header value")
}

fn mime_for(path: &Path) -> HeaderValue {
    mime_guess::from_path(path)
        .first_raw()
        .map(HeaderValue::from_static)
        .unwrap_or_else(|| HeaderValue::from_str(mime::APPLICATION_OCTET_STREAM.as_ref()).unwrap())
}

/// Append `.br` to the full file name (e.g. `app.js` -> `app.js.br`).
fn with_br_ext(path: &Path) -> PathBuf {
    let mut name = OsString::from(path.as_os_str());
    name.push(".br");
    PathBuf::from(name)
}

/// A resolved file plus the metadata needed to build the response.
struct Resolved {
    file: &'static File<'static>,
    mime: HeaderValue,
    brotli: bool,
    /// The logical (non-`.br`) path the response represents; used to key settings.
    logical: PathBuf,
}

fn resolve_file(
    dir: &'static Dir<'static>,
    path: &Path,
    brotli: bool,
    redirect_not_found_to_index_html: bool,
) -> Option<Resolved> {
    if let Some(resolved) = try_resolve(dir, path, brotli) {
        return Some(resolved);
    }
    if redirect_not_found_to_index_html {
        return try_resolve(dir, Path::new("index.html"), brotli);
    }
    None
}

/// Resolve a single logical path, preferring the precompressed `.br` sibling when `brotli`.
fn try_resolve(dir: &'static Dir<'static>, logical: &Path, brotli: bool) -> Option<Resolved> {
    // Content-Type is derived from the logical extension, not the `.br` one.
    let mime = mime_for(logical);

    if brotli {
        if let Some(file) = dir.get_file(with_br_ext(logical)) {
            return Some(Resolved {
                file,
                mime,
                brotli: true,
                logical: logical.to_path_buf(),
            });
        }
    }

    let file = dir.get_file(logical)?;
    Some(Resolved {
        file,
        mime,
        brotli: false,
        logical: logical.to_path_buf(),
    })
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

enum Inner {
    File {
        file: &'static File<'static>,
        mime: HeaderValue,
        brotli: bool,
        vary_accept_encoding: bool,
        chunk_size: usize,
        cache_control: Option<HeaderValue>,
        headers: Vec<(HeaderName, HeaderValue)>,
    },
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
            Inner::File {
                file,
                mime,
                brotli,
                vary_accept_encoding,
                chunk_size,
                cache_control,
                headers,
            } => {
                let body = AsyncReadBody::with_capacity(file.contents(), chunk_size).boxed();
                let body = ResponseBody(body);

                let mut res = Response::new(body);
                let res_headers = res.headers_mut();
                res_headers.insert(header::CONTENT_TYPE, mime);
                if brotli {
                    res_headers.insert(header::CONTENT_ENCODING, HeaderValue::from_static("br"));
                }
                if vary_accept_encoding {
                    res_headers.insert(header::VARY, HeaderValue::from_static("accept-encoding"));
                }
                if let Some(cache_control) = cache_control {
                    res_headers.insert(header::CACHE_CONTROL, cache_control);
                }
                for (name, value) in headers {
                    res_headers.insert(name, value);
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

fn accepts_brotli(headers: &http::HeaderMap) -> bool {
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

    static CLIENT_SERVE_SETTINGS: OnceLock<ServeSettingsMap> = OnceLock::new();
    static ASSETS_DIR: Dir<'static> = include_dir!("$CARGO_MANIFEST_DIR/tests/assets");

    fn shared_settings() -> &'static ServeSettingsMap {
        CLIENT_SERVE_SETTINGS.get_or_init(|| ServeSettingsMap::with_hasher(Xxh3Builder::default()))
    }

    /// A fresh, isolated settings map (leaked to `'static`) for tests that mutate it.
    fn fresh_settings() -> &'static ServeSettingsMap {
        Box::leak(Box::new(ServeSettingsMap::with_hasher(Xxh3Builder::default())))
    }

    #[tokio::test]
    async fn basic() {
        let svc = ServeDir::new(&ASSETS_DIR, shared_settings());

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

    #[tokio::test]
    async fn cache_control_for_file() {
        let svc = ServeDir::new(&ASSETS_DIR, fresh_settings()).cache_control("text.txt", "no-cache");

        let req = Request::builder()
            .uri("/text.txt")
            .body(http_body_util::Empty::<Bytes>::new())
            .unwrap();
        let res = svc.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(res.headers()[header::CACHE_CONTROL], "no-cache");
    }

    #[tokio::test]
    async fn cache_control_for_folder_is_recursive() {
        let svc = ServeDir::new(&ASSETS_DIR, fresh_settings())
            .cache_control("subfolder", "public, max-age=31536000, immutable");

        let req = Request::builder()
            .uri("/subfolder/data.json")
            .body(http_body_util::Empty::<Bytes>::new())
            .unwrap();
        let res = svc.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers()[header::CACHE_CONTROL],
            "public, max-age=31536000, immutable"
        );
    }

    #[tokio::test]
    async fn default_settings_apply_and_specific_overrides() {
        let svc = ServeDir::new(&ASSETS_DIR, fresh_settings())
            .default_cache_control("no-cache")
            .default_header(
                HeaderName::from_static("x-content-type-options"),
                HeaderValue::from_static("nosniff"),
            )
            .cache_control("subfolder/data.json", "immutable");

        // default applies to a file with no specific entry
        let res = ServeDir::clone(&svc)
            .oneshot(
                Request::builder()
                    .uri("/text.txt")
                    .body(http_body_util::Empty::<Bytes>::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.headers()[header::CACHE_CONTROL], "no-cache");
        assert_eq!(res.headers()["x-content-type-options"], "nosniff");

        // specific cache_control overrides the default; default header still applies
        let res = svc
            .oneshot(
                Request::builder()
                    .uri("/subfolder/data.json")
                    .body(http_body_util::Empty::<Bytes>::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.headers()[header::CACHE_CONTROL], "immutable");
        assert_eq!(res.headers()["x-content-type-options"], "nosniff");
    }

    #[tokio::test]
    async fn immutable_convenience_sets_long_cache() {
        let svc = ServeDir::new(&ASSETS_DIR, fresh_settings()).immutable("text.txt");

        let req = Request::builder()
            .uri("/text.txt")
            .body(http_body_util::Empty::<Bytes>::new())
            .unwrap();
        let res = svc.oneshot(req).await.unwrap();

        assert_eq!(
            res.headers()[header::CACHE_CONTROL],
            "public, max-age=31536000, immutable"
        );
    }

    #[tokio::test]
    async fn brotli_enabled_sets_vary_even_when_uncompressed() {
        // No `.br` sibling exists, so the uncompressed file is served, but the resource is
        // still content-negotiated and must advertise `Vary: Accept-Encoding`.
        let svc = ServeDir::new(&ASSETS_DIR, shared_settings()).precompressed_br();

        let req = Request::builder()
            .uri("/text.txt")
            .header(header::ACCEPT_ENCODING, "br")
            .body(http_body_util::Empty::<Bytes>::new())
            .unwrap();
        let res = svc.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        assert!(res.headers().get(header::CONTENT_ENCODING).is_none());
        assert_eq!(res.headers()[header::VARY], "accept-encoding");
    }

    #[tokio::test]
    async fn no_vary_when_brotli_disabled() {
        let svc = ServeDir::new(&ASSETS_DIR, shared_settings());

        let req = Request::builder()
            .uri("/text.txt")
            .body(http_body_util::Empty::<Bytes>::new())
            .unwrap();
        let res = svc.oneshot(req).await.unwrap();

        assert!(!res.headers().contains_key(header::VARY));
    }

    #[cfg(feature = "metadata")]
    #[tokio::test]
    async fn with_if_modified_since() {
        let svc = ServeDir::new(&ASSETS_DIR, shared_settings());

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
        let svc = ServeDir::new(&ASSETS_DIR, shared_settings()).with_buf_chunk_size(1024 * 32);

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
        let svc = ServeDir::new(&ASSETS_DIR, shared_settings());

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
        let svc = ServeDir::new(&ASSETS_DIR, shared_settings());

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
        let svc = ServeDir::new(&ASSETS_DIR, shared_settings());

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
        let svc = ServeDir::new(&ASSETS_DIR, shared_settings()).append_index_html_on_directories(false);

        let req = Request::new(http_body_util::Empty::<Bytes>::new());
        let res = svc.oneshot(req).await.unwrap();

        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        assert!(res.headers().get(header::CONTENT_TYPE).is_none());

        let body = body_into_text(res.into_body()).await;
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn root_path_with_index() {
        let svc = ServeDir::new(&ASSETS_DIR, shared_settings());

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
        let bytes = body.collect().await.unwrap().to_bytes();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn access_cjk_percent_encoded_uri_path() {
        let svc = ServeDir::new(&ASSETS_DIR, shared_settings());

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
        let svc = ServeDir::new(&ASSETS_DIR, shared_settings());

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
