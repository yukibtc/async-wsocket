// Copyright (c) 2022-2024 Yuki Kishimoto
// Distributed under the MIT software license

//! Native

#[cfg(feature = "socks")]
use std::net::SocketAddr;

use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
pub use tokio_tungstenite::tungstenite::http::{HeaderMap, HeaderName, HeaderValue};
pub use tokio_tungstenite::tungstenite::protocol::{Role, WebSocketConfig};
pub use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::MaybeTlsStream;
pub use tokio_tungstenite::{accept_async, accept_async_with_config, WebSocketStream};
use url::Url;

mod error;
#[cfg(feature = "socks")]
mod socks;

pub use self::error::Error;
#[cfg(feature = "socks")]
use self::socks::TcpSocks5Stream;
use crate::socket::WebSocket;
use crate::ConnectionMode;

pub async fn connect(url: &Url, mode: &ConnectionMode) -> Result<WebSocket, Error> {
    connect_with_headers(url, mode, HeaderMap::new()).await
}

/// Connect with additional HTTP headers in the WebSocket upgrade request.
pub async fn connect_with_headers(
    url: &Url,
    mode: &ConnectionMode,
    headers: HeaderMap,
) -> Result<WebSocket, Error> {
    match mode {
        ConnectionMode::Direct => connect_direct(url, headers).await,
        #[cfg(feature = "socks")]
        ConnectionMode::Proxy(proxy) => connect_proxy(url, *proxy, headers).await,
    }
}

async fn connect_direct(url: &Url, headers: HeaderMap) -> Result<WebSocket, Error> {
    let host: &str = url.host_str().ok_or_else(Error::empty_host)?;
    let port: u16 = url
        .port_or_known_default()
        .ok_or_else(Error::invalid_port)?;

    let host: String = format!("{}:{}", host, port);

    let tcp_stream: TcpStream = tokio_happy_eyeballs::connect(host).await?;

    connect_stream(url, tcp_stream, headers).await
}

#[cfg(feature = "socks")]
async fn connect_proxy(
    url: &Url,
    proxy: SocketAddr,
    headers: HeaderMap,
) -> Result<WebSocket, Error> {
    let host: &str = url.host_str().ok_or_else(Error::empty_host)?;
    let port: u16 = url
        .port_or_known_default()
        .ok_or_else(Error::invalid_port)?;
    let addr: String = format!("{host}:{port}");

    let conn: TcpStream = TcpSocks5Stream::connect(proxy, addr).await?;
    connect_stream(url, conn, headers).await
}

async fn connect_stream(
    url: &Url,
    stream: TcpStream,
    headers: HeaderMap,
) -> Result<WebSocket, Error> {
    let stream = client_async(url, stream, headers).await?;
    Ok(WebSocket::tokio(Box::new(stream)))
}

// NOT REMOVE `Box::pin`!
// Use `Box::pin` to fix stack overflow on windows targets due to large `Future`
#[cfg(any(
    feature = "native-tls",
    feature = "native-tls-vendored",
    feature = "rustls-tls-native-roots",
    feature = "rustls-tls-webpki-roots"
))]
async fn client_async(
    url: &Url,
    stream: TcpStream,
    headers: HeaderMap,
) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Error> {
    let request = request_with_headers(url, headers)?;
    let (stream, _) = Box::pin(tokio_tungstenite::client_async_tls(request, stream)).await?;
    Ok(stream)
}

#[cfg(not(any(
    feature = "native-tls",
    feature = "native-tls-vendored",
    feature = "rustls-tls-native-roots",
    feature = "rustls-tls-webpki-roots"
)))]
async fn client_async(
    url: &Url,
    stream: TcpStream,
    headers: HeaderMap,
) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Error> {
    if url.scheme() == "wss" {
        return Err(tokio_tungstenite::tungstenite::Error::Url(
            tokio_tungstenite::tungstenite::error::UrlError::TlsFeatureNotEnabled,
        )
        .into());
    }

    let request = request_with_headers(url, headers)?;
    let (stream, _) = Box::pin(tokio_tungstenite::client_async(
        request,
        MaybeTlsStream::Plain(stream),
    ))
    .await?;
    Ok(stream)
}

fn request_with_headers(
    url: &Url,
    headers: HeaderMap,
) -> Result<tokio_tungstenite::tungstenite::handshake::client::Request, Error> {
    let mut request = url.as_str().into_client_request()?;
    request.headers_mut().extend(headers);
    Ok(request)
}

#[cfg(test)]
mod tests {
    use tokio::net::TcpListener;
    use tokio_tungstenite::tungstenite::handshake::server::Request;

    use super::*;

    #[test]
    fn request_with_headers_adds_headers_to_upgrade_request() {
        let url = Url::parse("wss://relay.example.com").unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("user-agent", HeaderValue::from_static("nostr-sdk"));

        let request = request_with_headers(&url, headers).unwrap();

        assert_eq!(request.headers().get("user-agent").unwrap(), "nostr-sdk");
        assert_eq!(request.headers().get("host").unwrap(), "relay.example.com");
    }

    #[tokio::test]
    #[allow(clippy::result_large_err)]
    async fn connect_with_headers_sends_headers_in_upgrade_request() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            tokio_tungstenite::accept_hdr_async(stream, |request: &Request, response| {
                assert_eq!(request.headers().get("user-agent").unwrap(), "nostr-sdk");
                Ok(response)
            })
            .await
            .unwrap();
        });

        let url = Url::parse(&format!("ws://{address}")).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("user-agent", HeaderValue::from_static("nostr-sdk"));

        connect_with_headers(&url, &ConnectionMode::Direct, headers)
            .await
            .unwrap();
        server.await.unwrap();
    }
}
