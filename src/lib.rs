// Copyright (c) 2022-2024 Yuki Kishimoto
// Distributed under the MIT software license

//! Async WebSocket

#![forbid(unsafe_code)]
#![warn(clippy::large_futures)]
#![cfg_attr(feature = "default", doc = include_str!("../README.md"))]

#[cfg(all(feature = "socks", not(target_arch = "wasm32")))]
use std::net::SocketAddr;

pub use futures_util;
pub use url::{self, Url};

pub mod message;
#[cfg(not(target_arch = "wasm32"))]
pub mod native;
pub mod prelude;
mod socket;
#[cfg(target_arch = "wasm32")]
pub mod wasm;

pub use self::message::Message;
#[cfg(not(target_arch = "wasm32"))]
pub use self::native::Error;
#[cfg(not(target_arch = "wasm32"))]
pub use self::native::{HeaderMap, HeaderName, HeaderValue};
pub use self::socket::WebSocket;
#[cfg(target_arch = "wasm32")]
pub use self::wasm::Error;

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ConnectionMode {
    /// Direct
    #[default]
    Direct,
    /// Custom proxy
    #[cfg(all(feature = "socks", not(target_arch = "wasm32")))]
    Proxy(SocketAddr),
}

impl ConnectionMode {
    /// Direct connection
    #[inline]
    pub fn direct() -> Self {
        Self::Direct
    }

    /// Proxy
    #[inline]
    #[cfg(all(feature = "socks", not(target_arch = "wasm32")))]
    pub fn proxy(addr: SocketAddr) -> Self {
        Self::Proxy(addr)
    }
}

/// Connect
#[inline]
pub async fn connect(url: &Url, mode: &ConnectionMode) -> Result<WebSocket, Error> {
    WebSocket::connect(url, mode).await
}

/// Connect with additional HTTP headers in the WebSocket upgrade request.
#[cfg(not(target_arch = "wasm32"))]
#[inline]
pub async fn connect_with_headers(
    url: &Url,
    mode: &ConnectionMode,
    headers: HeaderMap,
) -> Result<WebSocket, Error> {
    WebSocket::connect_with_headers(url, mode, headers).await
}
