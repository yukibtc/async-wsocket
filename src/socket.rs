// Copyright (c) 2022-2024 Yuki Kishimoto
// Distributed under the MIT software license

use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::{Sink, Stream};
#[cfg(not(target_arch = "wasm32"))]
use tokio::net::TcpStream;
#[cfg(not(target_arch = "wasm32"))]
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use url::Url;

#[cfg(target_arch = "wasm32")]
use crate::wasm::WsStream;
use crate::{ConnectionMode, Error, Message};

#[cfg(not(target_arch = "wasm32"))]
type WsStream<T> = WebSocketStream<MaybeTlsStream<T>>;

enum InnerWebSocket {
    #[cfg(not(target_arch = "wasm32"))]
    Tokio(Box<WsStream<TcpStream>>),
    #[cfg(target_arch = "wasm32")]
    Wasm(WsStream),
}

pub struct WebSocket {
    inner: InnerWebSocket,
}

impl WebSocket {
    #[inline]
    fn new(inner: InnerWebSocket) -> Self {
        Self { inner }
    }

    #[inline]
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn tokio(inner: Box<WsStream<TcpStream>>) -> Self {
        Self::new(InnerWebSocket::Tokio(inner))
    }
    #[inline]
    #[cfg(target_arch = "wasm32")]
    pub(crate) fn wasm(inner: WsStream) -> Self {
        Self::new(InnerWebSocket::Wasm(inner))
    }

    pub async fn connect(url: &Url, _mode: &ConnectionMode) -> Result<Self, Error> {
        #[cfg(not(target_arch = "wasm32"))]
        let socket: WebSocket = crate::native::connect(url, _mode).await?;

        #[cfg(target_arch = "wasm32")]
        let socket: WebSocket = crate::wasm::connect(url).await?;

        Ok(socket)
    }

    /// Connect with additional HTTP headers in the WebSocket upgrade request.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn connect_with_headers(
        url: &Url,
        mode: &ConnectionMode,
        headers: crate::native::HeaderMap,
    ) -> Result<Self, Error> {
        crate::native::connect_with_headers(url, mode, headers).await
    }
}

impl Sink<Message> for WebSocket {
    type Error = Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match &mut self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            InnerWebSocket::Tokio(s) => Pin::new(s.as_mut()).poll_ready(cx).map_err(Into::into),
            #[cfg(target_arch = "wasm32")]
            InnerWebSocket::Wasm(s) => Pin::new(s).poll_ready(cx),
        }
    }

    fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        match &mut self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            InnerWebSocket::Tokio(s) => Pin::new(s.as_mut())
                .start_send(item.into())
                .map_err(Into::into),
            #[cfg(target_arch = "wasm32")]
            InnerWebSocket::Wasm(s) => Pin::new(s).start_send(item),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match &mut self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            InnerWebSocket::Tokio(s) => Pin::new(s.as_mut()).poll_flush(cx).map_err(Into::into),
            #[cfg(target_arch = "wasm32")]
            InnerWebSocket::Wasm(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match &mut self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            InnerWebSocket::Tokio(s) => Pin::new(s.as_mut()).poll_close(cx).map_err(Into::into),
            #[cfg(target_arch = "wasm32")]
            InnerWebSocket::Wasm(s) => Pin::new(s).poll_close(cx).map_err(Into::into),
        }
    }
}

impl Stream for WebSocket {
    type Item = Result<Message, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            InnerWebSocket::Tokio(s) => Pin::new(s)
                .poll_next(cx)
                .map(|i| i.map(|res| res.map(Message::from_native)))
                .map_err(Into::into),
            #[cfg(target_arch = "wasm32")]
            InnerWebSocket::Wasm(s) => Pin::new(s).poll_next(cx).map_err(Into::into),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            InnerWebSocket::Tokio(s) => s.size_hint(),
            #[cfg(target_arch = "wasm32")]
            InnerWebSocket::Wasm(s) => s.size_hint(),
        }
    }
}

#[cfg(target_arch = "wasm32")]
#[allow(dead_code)]
fn assert_wasm_send(url: &Url, mode: &ConnectionMode) {
    fn assert_type<T: Send>() {}
    fn assert_value<T: Send>(_: T) {}

    assert_type::<WebSocket>();
    assert_value(WebSocket::connect(url, mode));
}
