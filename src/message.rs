// Copyright (c) 2022-2024 Yuki Kishimoto
// Distributed under the MIT software license

use std::{fmt, str};

use bytes::Bytes;
#[cfg(not(target_arch = "wasm32"))]
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
#[cfg(not(target_arch = "wasm32"))]
use tokio_tungstenite::tungstenite::protocol::{
    CloseFrame as TungsteniteCloseFrame, Message as TungsteniteMessage,
};
#[cfg(not(target_arch = "wasm32"))]
use tokio_tungstenite::tungstenite::Utf8Bytes as TungsteniteUtf8Bytes;
use utf8bytes::Utf8Bytes;

/// An enum representing the various forms of a WebSocket message.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Message {
    /// A text WebSocket message
    Text(Utf8Bytes),
    /// A binary WebSocket message
    Binary(Bytes),
    /// A ping message with the specified payload
    ///
    /// The payload here must have a length less than 125 bytes
    #[cfg(not(target_arch = "wasm32"))]
    Ping(Bytes),
    /// A pong message with the specified payload
    ///
    /// The payload here must have a length less than 125 bytes
    #[cfg(not(target_arch = "wasm32"))]
    Pong(Bytes),
    /// A close message with the optional close frame.
    #[cfg(not(target_arch = "wasm32"))]
    Close(Option<CloseFrame>),
}

impl Message {
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn from_native(msg: TungsteniteMessage) -> Self {
        match msg {
            TungsteniteMessage::Text(text) => {
                Self::Text(unsafe { Utf8Bytes::from_bytes_unchecked(text.into()) })
            }
            TungsteniteMessage::Binary(data) => Self::Binary(data),
            TungsteniteMessage::Ping(data) => Self::Ping(data),
            TungsteniteMessage::Pong(data) => Self::Pong(data),
            TungsteniteMessage::Close(frame) => Self::Close(frame.map(|f| f.into())),
            // SAFETY: from tungstenite docs: "you're not going to get this value while reading the message".
            // SAFETY: this conversion is used only in Stream trait, so when reading the messages.
            TungsteniteMessage::Frame(..) => unreachable!(),
        }
    }

    /// Get the length of the WebSocket message.
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::Text(string) => string.len(),
            Self::Binary(data) => data.len(),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Ping(data) => data.len(),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Pong(data) => data.len(),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Close(data) => data.as_ref().map(|d| d.reason.len()).unwrap_or(0),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Attempt to get a &str from the WebSocket message,
    /// this will try to convert binary data to utf8.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text(string) => Some(string.as_str()),
            Self::Binary(data) => str::from_utf8(data).ok(),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Ping(data) | Self::Pong(data) => str::from_utf8(data).ok(),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Close(None) => Some(""),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Close(Some(frame)) => Some(&frame.reason),
        }
    }
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(string) = self.as_text() {
            write!(f, "{string}")
        } else {
            write!(f, "Binary Data<length={}>", self.len())
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<Message> for TungsteniteMessage {
    fn from(msg: Message) -> Self {
        match msg {
            Message::Text(text) => {
                Self::Text(unsafe { TungsteniteUtf8Bytes::from_bytes_unchecked(text.into()) })
            }
            Message::Binary(data) => Self::Binary(data),
            Message::Ping(data) => Self::Ping(data),
            Message::Pong(data) => Self::Pong(data),
            Message::Close(frame) => Self::Close(frame.map(|f| f.into())),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CloseFrame {
    /// The reason as a code.
    pub code: u16,
    /// The reason as text string.
    pub reason: Utf8Bytes,
}

#[cfg(not(target_arch = "wasm32"))]
impl From<CloseFrame> for TungsteniteCloseFrame {
    fn from(frame: CloseFrame) -> Self {
        Self {
            code: CloseCode::from(frame.code),
            reason: unsafe { TungsteniteUtf8Bytes::from_bytes_unchecked(frame.reason.into()) },
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<TungsteniteCloseFrame> for CloseFrame {
    fn from(frame: TungsteniteCloseFrame) -> Self {
        Self {
            code: frame.code.into(),
            reason: unsafe { Utf8Bytes::from_bytes_unchecked(frame.reason.into()) },
        }
    }
}
