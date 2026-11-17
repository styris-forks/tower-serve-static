use crate::conditional::{evaluate, Outcome};
use bytes::Bytes;
use http::{
    header::{
        ACCEPT_RANGES, ALLOW, CACHE_CONTROL, CONTENT_LENGTH, CONTENT_RANGE, ETAG, LAST_MODIFIED,
        VARY,
    },
    HeaderMap, HeaderValue, Method, Request, Response, StatusCode,
};
use http_body::Frame;
use http_body_util::Full;
use std::{convert::Infallible, time::SystemTime};

opaque_body! {
    /// Response body of the services in this crate. It always knows its exact length, so
    /// responses carry a `Content-Length`.
    pub type ResponseBody = Full<Bytes>;
}

/// Response future of the services in this crate. Embedded files are always ready.
pub type ResponseFuture = std::future::Ready<Result<Response<ResponseBody>, Infallible>>;

/// A quoted, strong entity tag for `bytes`, so identical content always shares one.
pub(crate) fn etag(bytes: &[u8]) -> HeaderValue {
    let hash = xxhash_rust::xxh3::xxh3_64(bytes);
    HeaderValue::from_str(&format!("\"{hash:016x}\"")).expect("hex is a valid header value")
}

fn build(status: StatusCode, headers: HeaderMap, body: Bytes) -> Response<ResponseBody> {
    let mut res = Response::new(ResponseBody(Full::new(body)));
    *res.status_mut() = status;
    *res.headers_mut() = headers;
    res
}

pub(crate) fn empty(status: StatusCode) -> Response<ResponseBody> {
    build(status, HeaderMap::new(), Bytes::new())
}

/// Static files only answer `GET` and `HEAD`. Anything else gets `405` naming those.
pub(crate) fn method_not_allowed<B>(req: &Request<B>) -> Option<Response<ResponseBody>> {
    if matches!(*req.method(), Method::GET | Method::HEAD) {
        return None;
    }
    let headers = HeaderMap::from_iter([(ALLOW, HeaderValue::from_static("GET, HEAD"))]);
    Some(build(StatusCode::METHOD_NOT_ALLOWED, headers, Bytes::new()))
}

/// Answers `req` with `bytes`, or the part of them it asked for, under the representation
/// `headers` (`Content-Type`, `Content-Encoding`, `Vary`, `Cache-Control` and any extras).
pub(crate) fn respond<B>(
    req: &Request<B>,
    bytes: &'static [u8],
    etag: HeaderValue,
    last_modified: Option<SystemTime>,
    mut headers: HeaderMap,
) -> Response<ResponseBody> {
    let len = bytes.len() as u64;
    let outcome = evaluate(
        req.method(),
        req.headers(),
        etag.to_str().expect("entity tags are ASCII"),
        last_modified,
        len,
    );

    let mut validators = HeaderMap::new();
    validators.insert(ETAG, etag);
    validators.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if let Some(modified) = last_modified {
        let modified = httpdate::fmt_http_date(modified);
        validators.insert(
            LAST_MODIFIED,
            HeaderValue::from_str(&modified).expect("HTTP dates are valid header values"),
        );
    }
    let content_range = |range: String| {
        HeaderValue::from_str(&format!("bytes {range}"))
            .expect("byte ranges are valid header values")
    };

    let (status, body) = match outcome {
        Outcome::PreconditionFailed => {
            return build(StatusCode::PRECONDITION_FAILED, validators, Bytes::new())
        }
        Outcome::NotModified => {
            // A 304 stands in for the 200, so caches need the headers that steer them
            for name in [CACHE_CONTROL, VARY] {
                if let Some(value) = headers.remove(&name) {
                    validators.insert(name, value);
                }
            }
            return build(StatusCode::NOT_MODIFIED, validators, Bytes::new());
        }
        Outcome::RangeNotSatisfiable => {
            validators.insert(CONTENT_RANGE, content_range(format!("*/{len}")));
            return build(StatusCode::RANGE_NOT_SATISFIABLE, validators, Bytes::new());
        }
        Outcome::Full => (StatusCode::OK, bytes),
        Outcome::Partial(range) => {
            let (start, end) = range.into_inner();
            validators.insert(CONTENT_RANGE, content_range(format!("{start}-{end}/{len}")));
            (
                StatusCode::PARTIAL_CONTENT,
                &bytes[start as usize..=end as usize],
            )
        }
    };

    headers.extend(validators);
    headers.insert(CONTENT_LENGTH, HeaderValue::from(body.len()));
    // HEAD gets the headers GET would, Content-Length included, but no body
    let body = if req.method() == Method::HEAD {
        Bytes::new()
    } else {
        Bytes::from_static(body)
    };
    build(status, headers, body)
}
