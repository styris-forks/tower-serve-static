//! Conditional and range request evaluation ([RFC 9110] sections 13 and 14).
//!
//! The services in this crate use it for every file they serve, and it's public so a body served
//! some other way (a file on disk, a blob store) can answer the same headers the same way.
//!
//! [RFC 9110]: https://www.rfc-editor.org/rfc/rfc9110

use http::{
    header::{IF_MATCH, IF_MODIFIED_SINCE, IF_NONE_MATCH, IF_RANGE, IF_UNMODIFIED_SINCE, RANGE},
    HeaderMap, HeaderName, Method,
};
use http_range_header::SyntacticallyCorrectRange;
use httpdate::HttpDate;
use std::{ops::RangeInclusive, time::SystemTime};

/// How to answer a request, decided by [`evaluate`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// `412 Precondition Failed`, since `If-Match` or `If-Unmodified-Since` failed, or an
    /// unsafe method's `If-None-Match` matched.
    PreconditionFailed,
    /// `304 Not Modified`.
    NotModified,
    /// `416 Range Not Satisfiable`, sent with `Content-Range: bytes */<len>`.
    RangeNotSatisfiable,
    /// `200 OK` with the whole body.
    Full,
    /// `206 Partial Content` with these bytes, sent with `Content-Range: bytes <start>-<end>/<len>`.
    Partial(RangeInclusive<u64>),
}

/// Answers `If-Match`, `If-Unmodified-Since`, `If-None-Match`, `If-Modified-Since`, `Range` and
/// `If-Range` for a body of `len` bytes tagged with the quoted, strong `etag`, which has to change
/// whenever the bytes do. The preconditions are evaluated in the order RFC 9110 section 13.2.2
/// gives, and a date one is skipped when its tag counterpart is present or `last_modified` is
/// unknown.
///
/// A `Range` header that doesn't parse (a backwards range included) is ignored, as the spec asks,
/// while one with no range reaching into the body is refused. A suffix longer than the body asks
/// for all of it. Ranges that touch or overlap are served as one, and ones with a gap between them
/// get the whole body, since a single body can't carry them. Only `GET` is ever given a range.
pub fn evaluate(
    method: &Method,
    request: &HeaderMap,
    etag: &str,
    last_modified: Option<SystemTime>,
    len: u64,
) -> Outcome {
    let last_modified = last_modified.map(HttpDate::from);
    let since = |name| header_date(request, name).zip(last_modified);
    let safe = matches!(*method, Method::GET | Method::HEAD);

    let unchanged = match etag_listed(request, IF_MATCH, etag, false) {
        Some(listed) => listed,
        None => !since(IF_UNMODIFIED_SINCE).is_some_and(|(since, modified)| modified > since),
    };
    if !unchanged {
        return Outcome::PreconditionFailed;
    }

    let not_modified = match etag_listed(request, IF_NONE_MATCH, etag, true) {
        // A write conditioned on the resource not existing in this state has to fail outright
        Some(true) if !safe => return Outcome::PreconditionFailed,
        Some(listed) => listed,
        None => safe && since(IF_MODIFIED_SINCE).is_some_and(|(since, modified)| modified <= since),
    };
    if not_modified {
        return Outcome::NotModified;
    }

    if method != Method::GET || len == 0 {
        return Outcome::Full;
    }
    select_range(request, etag, last_modified, len)
}

fn entity_tags(mut rest: &str) -> impl Iterator<Item = &str> {
    std::iter::from_fn(move || {
        rest = rest.trim_start_matches([' ', '\t', ',']);
        let end = if rest.starts_with('*') {
            1
        } else {
            let opaque = if rest.starts_with("W/") { 2 } else { 0 };
            if !rest[opaque..].starts_with('"') {
                return None;
            }
            opaque + 1 + rest[opaque + 1..].find('"')? + 1
        };
        let (tag, tail) = rest.split_at(end);
        rest = tail;
        Some(tag)
    })
}

/// Whether `etag` is among the tags in the `name` headers, or `None` when there are none.
fn etag_listed(request: &HeaderMap, name: HeaderName, etag: &str, weak: bool) -> Option<bool> {
    let mut values = request.get_all(name).iter().peekable();
    values.peek()?;
    Some(
        values
            .filter_map(|value| value.to_str().ok())
            .flat_map(entity_tags)
            .any(|tag| {
                let tag = if weak {
                    tag.strip_prefix("W/").unwrap_or(tag)
                } else {
                    tag
                };
                tag == "*" || tag == etag
            }),
    )
}

/// The date in the `name` header, unless it's missing, repeated or not a valid HTTP date, in
/// which case the spec has it ignored.
fn header_date(request: &HeaderMap, name: HeaderName) -> Option<HttpDate> {
    let mut values = request.get_all(name).iter();
    let (Some(value), None) = (values.next(), values.next()) else {
        return None;
    };
    value.to_str().ok()?.parse().ok()
}

fn select_range(
    request: &HeaderMap,
    etag: &str,
    last_modified: Option<HttpDate>,
    len: u64,
) -> Outcome {
    let mut values = request.get_all(RANGE).iter();
    let (Some(value), None) = (values.next(), values.next()) else {
        return Outcome::Full;
    };
    // If-Range sends back one validator as it was given, so it has to match exactly
    let last_modified = last_modified.map(|date| date.to_string());
    let current = request.get_all(IF_RANGE).iter().all(|value| {
        let value = value.as_bytes().trim_ascii();
        value == etag.as_bytes()
            || last_modified
                .as_ref()
                .is_some_and(|date| value == date.as_bytes())
    });
    if !current {
        return Outcome::Full;
    }
    let Some(parsed) = value
        .to_str()
        .ok()
        .and_then(|value| http_range_header::parse_range_header(value).ok())
    else {
        return Outcome::Full;
    };
    let last = len - 1;
    let mut ranges: Vec<_> = parsed
        .into_ranges()
        .into_iter()
        .map(|range| match range {
            SyntacticallyCorrectRange::FromLast(suffix) => (len.saturating_sub(suffix), last),
            SyntacticallyCorrectRange::FromStart(start) => (start, last),
            SyntacticallyCorrectRange::Indexed(start, end) => (start, end.min(last)),
        })
        .filter(|&(start, _)| start <= last)
        .collect();
    ranges.sort_unstable();
    let mut ranges = ranges.into_iter();
    let Some((start, mut end)) = ranges.next() else {
        return Outcome::RangeNotSatisfiable;
    };
    for (next_start, next_end) in ranges {
        if next_start > end.saturating_add(1) {
            return Outcome::Full;
        }
        end = end.max(next_end);
    }
    Outcome::Partial(start..=end)
}
