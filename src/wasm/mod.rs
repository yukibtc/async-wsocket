// Copyright (c) 2019-2022 Naja Melan
// Copyright (c) 2023-2024 Yuki Kishimoto
// Distributed under the MIT software license

//! Wasm

use url::Url;

mod error;
mod event;
mod message;
mod state;
mod stream;

pub use self::error::Error;
use self::event::CloseEvent;
use self::state::WsState;
pub(crate) use self::stream::WsStream;
use crate::socket::WebSocket;

pub async fn connect(url: &Url) -> Result<WebSocket, Error> {
    let stream = WsStream::connect(url).await?;
    Ok(WebSocket::wasm(stream))
}
