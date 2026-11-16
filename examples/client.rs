// Copyright (c) 2022-2024 Yuki Kishimoto
// Distributed under the MIT software license

use async_wsocket::prelude::*;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};

const NONCE: Bytes = Bytes::from_static(&123456789u64.to_be_bytes());

#[tokio::main]
async fn main() {
    let url = Url::parse("wss://relay.damus.io").unwrap();
    let mut socket: WebSocket = WebSocket::connect(&url, &ConnectionMode::direct())
        .await
        .unwrap();

    // Split sink and stream
    // let (mut tx, mut rx) = socket.split();

    // Send ping
    socket.send(Message::Ping(NONCE)).await.unwrap();

    // Listen for messages
    while let Some(msg) = socket.next().await {
        if let Ok(Message::Pong(bytes)) = msg {
            assert_eq!(NONCE, bytes);
            println!("Pong match!");
            break;
        }
    }
}
