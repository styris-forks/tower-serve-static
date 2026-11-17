//! Tower file services embedding assets into the binary.
//!
//! Both services answer `GET` and `HEAD` only (anything else gets `405`) and send an `ETag`
//! hashed from the content, a `Content-Length`, and `Accept-Ranges: bytes`. They answer
//! `If-Match`, `If-None-Match`, `If-Modified-Since`, `Range` and `If-Range` through
//! [`conditional::evaluate`], which is also public for bodies served some other way.
//!
//! # Serve Static File
//!
//! ```
//! use tower_serve_static::{ServeFile, include_file};
//!
//! // File is located relative to `CARGO_MANIFEST_DIR` (the directory containing the manifest of your package).
//! // This will embed and serve the `README.md` file.
//! let service = ServeFile::new(include_file!("/README.md"));
//!
//! // Run our service using `axum`
//! let app = axum::Router::new().fallback_service(service);
//!
//! # async {
//! // run our app with axum, listening locally on port 3000
//! let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
//! axum::serve(listener, app).await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # };
//! ```
//!
//! # Serve Static Directory
//!
//! ```
//! use tower_serve_static::{ServeDir, ServeSettingsMap};
//! use include_dir::{Dir, include_dir};
//! use std::sync::OnceLock;
//! use xxhash_rust::xxh3::Xxh3Builder;
//!
//!
//! // Use `$CARGO_MANIFEST_DIR` to make path relative to your package.
//! // This will embed and serve files in the `src` directory and its subdirectories.
//! static ASSETS_DIR: Dir<'static> = include_dir!("$CARGO_MANIFEST_DIR/src");
//! static SERVE_SETTINGS: OnceLock<ServeSettingsMap> = OnceLock::new();
//! let service = ServeDir::new(
//!     &ASSETS_DIR,
//!     SERVE_SETTINGS.get_or_init(|| ServeSettingsMap::with_hasher(Xxh3Builder::default())),
//! )
//! // serve `index.html` uncached; fingerprinted assets get a long, immutable cache.
//! .default_cache_control("no-cache")
//! .cache_control("assets", "public, max-age=31536000, immutable");
//!
//! // Run our service using `axum`
//! let app = axum::Router::new().fallback_service(service);
//!
//! // run our app with axum, listening locally on port 3000
//! # async {
//! let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
//! axum::serve(listener, app).await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # };
//! ```
//!
//! # Features
//!
//! This library exposes the following features that can be enabled:
//!
//! - `metadata` - enables [`ServeDir`] to send the embedded files' `Last-Modified` and to answer
//!   `If-Modified-Since` and date-based `If-Range` with it.

#![deny(rust_2018_idioms, missing_docs)]

#[macro_use]
mod macros;

pub mod conditional;
mod response;
mod serve_dir;
mod serve_file;

#[doc(hidden)]
pub mod private {
    pub use {http, mime, mime_guess};
}

pub use self::{
    response::{ResponseBody, ResponseFuture},
    serve_dir::{ServeDir, ServeSettings, ServeSettingsMap},
    serve_file::{File, ServeFile},
};
