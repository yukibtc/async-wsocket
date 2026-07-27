// Copyright (c) 2019-2022 Naja Melan
// Copyright (c) 2023-2024 Yuki Kishimoto
// Distributed under the MIT software license

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::channel::{mpsc, oneshot};
use futures::prelude::{Sink, Stream};
use futures::{ready, StreamExt};
use url::Url;
use wasm_bindgen::closure::Closure;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::spawn_local;
use web_sys::{
    BinaryType, CloseEvent as JsCloseEvent, DomException, MessageEvent, WebSocket as JsWebSocket,
};

use crate::message::Message;
use crate::wasm::{CloseEvent, Error, WsState};

type SendResult = oneshot::Receiver<Result<(), Error>>;

enum Command {
    Send {
        message: Message,
        result: oneshot::Sender<Result<(), Error>>,
    },
    Close,
}

enum LifecycleEvent {
    Open,
    Closed(CloseEvent),
    Failed(Error),
}

enum StreamEvent {
    Message(Result<Message, Error>),
    Closed,
}

struct PendingConnection {
    commands: mpsc::UnboundedSender<Command>,
    lifecycle: mpsc::UnboundedReceiver<LifecycleEvent>,
    messages: mpsc::UnboundedReceiver<StreamEvent>,
    url: String,
}

/// A futures `Sink` and `Stream` backed by the browser WebSocket API.
///
/// JavaScript objects remain on the worker that created the connection. Commands and
/// received messages cross threads through channels, making this handle safe to move
/// between WASM threads.
pub struct WsStream {
    commands: mpsc::UnboundedSender<Command>,
    lifecycle: mpsc::UnboundedReceiver<LifecycleEvent>,
    messages: mpsc::UnboundedReceiver<StreamEvent>,
    send_result: Option<SendResult>,
    ready_state: WsState,
    close_requested: bool,
    url: String,
}

impl WsStream {
    pub(crate) async fn connect(url: &Url) -> Result<Self, Error> {
        let mut pending = Self::start_actor(url)?;

        match pending.lifecycle.next().await {
            Some(LifecycleEvent::Open) => Ok(Self {
                commands: pending.commands,
                lifecycle: pending.lifecycle,
                messages: pending.messages,
                send_result: None,
                ready_state: WsState::Open,
                close_requested: false,
                url: pending.url,
            }),
            Some(LifecycleEvent::Closed(event)) => Err(Error::ConnectionFailed { event }),
            Some(LifecycleEvent::Failed(error)) => Err(error),
            None => Err(Error::ConnectionNotOpen),
        }
    }

    fn start_actor(url: &Url) -> Result<PendingConnection, Error> {
        let ws = Self::create_websocket(url)?;
        let socket_url = ws.url();
        let (command_tx, mut command_rx) = mpsc::unbounded();
        let (lifecycle_tx, lifecycle_rx) = mpsc::unbounded();
        let (message_tx, message_rx) = mpsc::unbounded();

        let open_events = lifecycle_tx.clone();
        let on_open = Closure::wrap(Box::new(move || {
            let _ = open_events.unbounded_send(LifecycleEvent::Open);
        }) as Box<dyn FnMut()>);

        let messages = message_tx.clone();
        let on_message = Closure::wrap(Box::new(move |event: MessageEvent| {
            let message = Message::try_from(event);
            let _ = messages.unbounded_send(StreamEvent::Message(message));
        }) as Box<dyn FnMut(MessageEvent)>);

        let close_events = lifecycle_tx.clone();
        let closed_messages = message_tx.clone();
        let on_close = Closure::wrap(Box::new(move |event: JsCloseEvent| {
            let _ = close_events.unbounded_send(LifecycleEvent::Closed(event.into()));
            let _ = closed_messages.unbounded_send(StreamEvent::Closed);
        }) as Box<dyn FnMut(JsCloseEvent)>);

        ws.set_binary_type(BinaryType::Arraybuffer);
        ws.set_onopen(Some(on_open.as_ref().unchecked_ref()));
        ws.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
        ws.set_onclose(Some(on_close.as_ref().unchecked_ref()));

        spawn_local(async move {
            let _callbacks = (on_open, on_message, on_close);

            while let Some(command) = command_rx.next().await {
                match command {
                    Command::Send { message, result } => {
                        let send_result = match message {
                            Message::Binary(data) => ws.send_with_u8_array(&data),
                            Message::Text(text) => ws.send_with_str(&text),
                        }
                        .map_err(|_| Error::ConnectionNotOpen);

                        let _ = result.send(send_result);
                    }
                    Command::Close => {
                        if ws.close().is_err() {
                            let error = Error::ConnectionNotOpen;
                            let _ =
                                lifecycle_tx.unbounded_send(LifecycleEvent::Failed(error.clone()));
                            let _ = message_tx.unbounded_send(StreamEvent::Message(Err(error)));
                            let _ = message_tx.unbounded_send(StreamEvent::Closed);
                        }
                    }
                }
            }

            ws.set_onopen(None);
            ws.set_onmessage(None);
            ws.set_onclose(None);

            if !matches!(ws.ready_state().try_into(), Ok(WsState::Closed)) {
                let _ = ws.close();
            }
        });

        Ok(PendingConnection {
            commands: command_tx,
            lifecycle: lifecycle_rx,
            messages: message_rx,
            url: socket_url,
        })
    }

    fn create_websocket(url: &Url) -> Result<JsWebSocket, Error> {
        JsWebSocket::new(url.as_str()).map_err(|value| {
            if let Some(exception) = value.dyn_ref::<DomException>() {
                if exception.code() == DomException::SYNTAX_ERR {
                    return Error::InvalidUrl {
                        supplied: url.to_string(),
                    };
                }

                if exception.code() != 0 {
                    return Error::Dom(exception.code());
                }
            }

            Error::Other(
                value
                    .as_string()
                    .unwrap_or_else(|| String::from("WebSocket construction failed")),
            )
        })
    }

    fn poll_send_result(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        let Some(result) = self.send_result.as_mut() else {
            return Poll::Ready(Ok(()));
        };

        let poll_result = match Pin::new(result).poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Ok(result)) => result,
            Poll::Ready(Err(_)) => Err(Error::ConnectionNotOpen),
        };

        self.send_result = None;
        Poll::Ready(poll_result)
    }

    fn poll_lifecycle(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        loop {
            match Pin::new(&mut self.lifecycle).poll_next(cx) {
                Poll::Ready(Some(LifecycleEvent::Open)) => {
                    self.ready_state = WsState::Open;
                }
                Poll::Ready(Some(LifecycleEvent::Closed(_))) => {
                    self.ready_state = WsState::Closed;
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Some(LifecycleEvent::Failed(error))) => {
                    self.ready_state = WsState::Closed;
                    return Poll::Ready(Err(error));
                }
                Poll::Ready(None) => {
                    self.ready_state = WsState::Closed;
                    return Poll::Ready(Err(Error::ConnectionNotOpen));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl fmt::Debug for WsStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WsStream for connection: {}", self.url)
    }
}

impl Drop for WsStream {
    fn drop(&mut self) {
        if !self.close_requested && self.ready_state != WsState::Closed {
            let _ = self.commands.unbounded_send(Command::Close);
        }
    }
}

impl Stream for WsStream {
    type Item = Result<Message, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.messages).poll_next(cx) {
            Poll::Ready(Some(StreamEvent::Message(message))) => Poll::Ready(Some(message)),
            Poll::Ready(Some(StreamEvent::Closed)) | Poll::Ready(None) => {
                self.ready_state = WsState::Closed;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Sink<Message> for WsStream {
    type Error = Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        ready!(self.poll_send_result(cx))?;

        match self.poll_lifecycle(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Err(Error::ConnectionNotOpen)),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending if self.ready_state == WsState::Open => Poll::Ready(Ok(())),
            Poll::Pending => Poll::Pending,
        }
    }

    fn start_send(mut self: Pin<&mut Self>, message: Message) -> Result<(), Self::Error> {
        if self.ready_state != WsState::Open || self.send_result.is_some() {
            return Err(Error::ConnectionNotOpen);
        }

        let (result_tx, result_rx) = oneshot::channel();
        self.commands
            .unbounded_send(Command::Send {
                message,
                result: result_tx,
            })
            .map_err(|_| Error::ConnectionNotOpen)?;
        self.send_result = Some(result_rx);

        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_send_result(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        ready!(self.poll_send_result(cx))?;

        match self.poll_lifecycle(cx) {
            Poll::Ready(result) => return Poll::Ready(result),
            Poll::Pending => {}
        }

        if !self.close_requested {
            self.ready_state = WsState::Closing;

            if self.commands.unbounded_send(Command::Close).is_err() {
                self.ready_state = WsState::Closed;
                return Poll::Ready(Err(Error::ConnectionNotOpen));
            }

            self.close_requested = true;
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use futures::task::noop_waker;

    use super::*;

    fn test_stream() -> (
        WsStream,
        mpsc::UnboundedSender<LifecycleEvent>,
        mpsc::UnboundedSender<StreamEvent>,
    ) {
        let (command_tx, _command_rx) = mpsc::unbounded();
        let (lifecycle_tx, lifecycle_rx) = mpsc::unbounded();
        let (message_tx, message_rx) = mpsc::unbounded();

        (
            WsStream {
                commands: command_tx,
                lifecycle: lifecycle_rx,
                messages: message_rx,
                send_result: None,
                ready_state: WsState::Open,
                close_requested: false,
                url: String::from("wss://example.com"),
            },
            lifecycle_tx,
            message_tx,
        )
    }

    #[test]
    fn close_event_updates_connection_state() {
        let (mut stream, lifecycle, _messages) = test_stream();
        let event = CloseEvent {
            code: 1000,
            reason: String::from("done"),
            was_clean: true,
        };
        lifecycle
            .unbounded_send(LifecycleEvent::Closed(event))
            .unwrap();
        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);

        assert_eq!(stream.poll_lifecycle(&mut context), Poll::Ready(Ok(())));
        assert_eq!(stream.ready_state, WsState::Closed);
    }

    #[test]
    fn received_messages_are_streamed_in_order() {
        let (mut stream, _lifecycle, messages) = test_stream();
        let first = Message::Text(String::from("first"));
        let second = Message::Binary(vec![1, 2, 3]);
        messages
            .unbounded_send(StreamEvent::Message(Ok(first.clone())))
            .unwrap();
        messages
            .unbounded_send(StreamEvent::Message(Ok(second.clone())))
            .unwrap();
        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);

        assert_eq!(
            Pin::new(&mut stream).poll_next(&mut context),
            Poll::Ready(Some(Ok(first)))
        );
        assert_eq!(
            Pin::new(&mut stream).poll_next(&mut context),
            Poll::Ready(Some(Ok(second)))
        );
    }

    #[test]
    fn close_event_ends_message_stream() {
        let (mut stream, _lifecycle, messages) = test_stream();
        messages.unbounded_send(StreamEvent::Closed).unwrap();
        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);

        assert_eq!(
            Pin::new(&mut stream).poll_next(&mut context),
            Poll::Ready(None)
        );
        assert_eq!(stream.ready_state, WsState::Closed);
    }
}
