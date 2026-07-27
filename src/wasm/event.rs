// Copyright (c) 2019-2022 Naja Melan
// Copyright (c) 2023-2024 Yuki Kishimoto
// Distributed under the MIT software license

use web_sys::CloseEvent as JsCloseEvt;

/// An event holding information about how/why the connection was closed.
///
/// See: [MDN Documentation](https://developer.mozilla.org/en-US/docs/Web/API/WebSocket/close).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloseEvent {
    /// The close code.
    pub code: u16,
    /// The reason why the connection was closed.
    pub reason: String,
    /// Whether the connection was closed cleanly.
    pub was_clean: bool,
}

impl From<JsCloseEvt> for CloseEvent {
    fn from(js_evt: JsCloseEvt) -> Self {
        Self {
            code: js_evt.code(),
            reason: js_evt.reason(),
            was_clean: js_evt.was_clean(),
        }
    }
}
