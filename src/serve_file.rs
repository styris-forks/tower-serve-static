use crate::response::{etag, method_not_allowed, respond, ResponseBody, ResponseFuture};
use http::{header, HeaderMap, HeaderValue, Request, Response};
use std::{
    convert::Infallible,
    task::{Context, Poll},
};
use tower_service::Service;

/// A file.
#[derive(Clone, Debug)]
pub struct File {
    bytes: &'static [u8],
    mime: HeaderValue,
}

impl File {
    /// Create a new [`File`].
    pub fn new(bytes: &'static [u8], mime: HeaderValue) -> Self {
        File { bytes, mime }
    }
}

/// Create a new [`File`].
///
/// The `Content-Type` will be guessed from the file extension.
#[macro_export]
macro_rules! include_file {
    ($file:expr) => {
        $crate::File::new(
            ::std::include_bytes!(::std::concat!(::std::env!("CARGO_MANIFEST_DIR"), $file)),
            $crate::private::mime_guess::from_path(&$file)
                .first_raw()
                .map(|mime| $crate::private::http::HeaderValue::from_static(mime))
                .unwrap_or_else(|| {
                    $crate::private::http::HeaderValue::from_str(
                        $crate::private::mime::APPLICATION_OCTET_STREAM.as_ref(),
                    )
                    .unwrap()
                }),
        )
    };
}

/// Create a new [`File`] with a specific mime type.
///
/// # Panics
///
/// Will panic if the mime type isn't a valid [header value].
///
/// [header value]: https://docs.rs/http/latest/http/header/struct.HeaderValue.html
#[macro_export]
macro_rules! include_file_with_mime {
    ($file:expr, $mime:expr) => {
        $crate::File::new(
            ::std::include_bytes!(::std::concat!(::std::env!("CARGO_MANIFEST_DIR"), $file)),
            $crate::private::http::HeaderValue::from_str($mime.as_ref())
                .expect("mime isn't a valid header value"),
        )
    };
}

/// Service that serves a file.
#[derive(Clone, Debug)]
pub struct ServeFile {
    file: File,
    etag: HeaderValue,
}

impl ServeFile {
    /// Create a new [`ServeFile`].
    pub fn new(file: File) -> Self {
        let etag = etag(file.bytes);
        Self { file, etag }
    }
}

impl<B> Service<Request<B>> for ServeFile {
    type Response = Response<ResponseBody>;
    type Error = Infallible;
    type Future = ResponseFuture;

    #[inline]
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        std::future::ready(Ok(method_not_allowed(&req).unwrap_or_else(|| {
            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, self.file.mime.clone());
            respond(&req, self.file.bytes, self.etag.clone(), None, headers)
        })))
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;
    use bytes::Bytes;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    #[tokio::test]
    async fn basic() {
        let svc = ServeFile::new(include_file!("/README.md"));

        let res = svc
            .oneshot(Request::new(http_body_util::Empty::<Bytes>::new()))
            .await
            .unwrap();

        assert_eq!(res.headers()["content-type"], "text/markdown");

        let body = res.into_body().collect().await.unwrap();
        let body = String::from_utf8(body.to_bytes().to_vec()).unwrap();

        assert!(body.starts_with("# Tower Serve Static"));
    }

    #[tokio::test]
    async fn with_mime() {
        let svc = ServeFile::new(include_file_with_mime!(
            "/README.md",
            mime::APPLICATION_OCTET_STREAM
        ));

        let res = svc
            .oneshot(Request::new(http_body_util::Empty::<Bytes>::new()))
            .await
            .unwrap();

        assert_eq!(res.headers()["content-type"], "application/octet-stream");

        let body = res.into_body().collect().await.unwrap();
        let body = String::from_utf8(body.to_bytes().to_vec()).unwrap();

        assert!(body.starts_with("# Tower Serve Static"));
    }

    // 404 is not possible with include_file!
    //
    // #[tokio::test]
    // async fn returns_404_if_file_doesnt_exist() {
    //     let svc = ServeFile::new(include_file!("/this-doesnt-exist.md"));

    //     let res = svc
    //         .oneshot(Request::new(http_body_util::Empty::<Bytes>::new()))
    //         .await
    //         .unwrap();

    //     assert_eq!(res.status(), http::StatusCode::NOT_FOUND);
    //     assert!(res.headers().get(header::CONTENT_TYPE).is_none());
    // }
}
