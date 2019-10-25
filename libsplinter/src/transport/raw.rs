// Copyright 2018 Cargill Incorporated
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;
use std::io::{ErrorKind, Read, Write};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use mio::{
    net::{TcpListener, TcpStream},
    Evented,
};

use crate::transport::{
    AcceptError, ConnectError, Connection, DisconnectError, ListenError, Listener, RecvError,
    SendError, Transport,
};

const PROTOCOL_PREFIX: &str = "tcp://";

#[derive(Debug)]
pub struct TransportCreateError(pub String);

impl std::error::Error for TransportCreateError {}

impl fmt::Display for TransportCreateError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug)]
pub struct TransportShutdownError(pub String);

impl std::error::Error for TransportShutdownError {}

impl fmt::Display for TransportShutdownError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&self.0)
    }
}

pub struct RawTransport {
    reactor_sender: mio_extras::channel::SyncSender<ReactorCtrl>,
    join_handle: Option<std::thread::JoinHandle<()>>,
}

impl RawTransport {
    pub fn new() -> Result<Self, TransportCreateError> {
        let (reactor_sender, reactor_receiver) = mio_extras::channel::sync_channel(1);
        let mut reactor = TcpReactor::new(reactor_receiver)
            .map_err(|err| TransportCreateError(format!("Unable to create reactor: {}", err)))?;

        let join_handle = std::thread::spawn(move || loop {
            match reactor.run() {
                Err(err) => {
                    error!("An issue occurred processing tcp connections: {:?}", err);
                    eprintln!("An issue occurred processing tcp connections: {:?}", err);
                }
                Ok(()) => break,
            }

            debug!("Exited RawTcpReactor thread");
            eprintln!("Exited RawTcpReactor thread");
        });

        Ok(Self {
            reactor_sender,
            join_handle: Some(join_handle),
        })
    }

    pub fn shutdown(mut self) -> Result<(), TransportShutdownError> {
        self.reactor_sender
            .send(ReactorCtrl::Shutdown)
            .map_err(|err| {
                TransportShutdownError(format!(
                    "Unable to send shutdown signal to reactor: {}",
                    err
                ))
            })?;

        let join_handle = self
            .join_handle
            .take()
            .ok_or_else(|| TransportShutdownError("Attempted to shutdown twice".into()))?;

        join_handle
            .join()
            .map_err(|_| TransportShutdownError("Unable to join reactor thread".into()))
    }
}

impl Drop for RawTransport {
    fn drop(&mut self) {
        if let Some(join_handle) = self.join_handle.take() {
            if let Err(_) = self.reactor_sender.send(ReactorCtrl::Shutdown) {
                // the thread probable already exited
            }

            if let Err(_) = join_handle.join() {
                error!("Failed to cleanly join raw transport thread");
            }
        }
    }
}

const DEFALT_INCOMING_QUEUE_SIZE: usize = 16;
const DEFALT_OUTGOING_QUEUE_SIZE: usize = 16;
const DEFALT_LISTENER_QUEUE_SIZE: usize = 16;

impl Transport for RawTransport {
    fn accepts(&self, address: &str) -> bool {
        address.starts_with(PROTOCOL_PREFIX) || !address.contains("://")
    }

    fn connect(&mut self, endpoint: &str) -> Result<Box<dyn Connection>, ConnectError> {
        if !self.accepts(endpoint) {
            return Err(ConnectError::ProtocolError(format!(
                "Invalid protocol \"{}\"",
                endpoint
            )));
        }

        eprintln!("creating connection to {}", endpoint);
        let address = if endpoint.starts_with(PROTOCOL_PREFIX) {
            &endpoint[PROTOCOL_PREFIX.len()..]
        } else {
            endpoint
        };
        let socket_addr = address
            .parse()
            .map_err(|err| ConnectError::ParseError(format!("invalid bind string: {}", err)))?;

        let socket = TcpStream::connect(&socket_addr)?;

        let local_endpoint = format!("tcp://{}", socket.local_addr().unwrap());
        let remote_endpoint = format!("tcp://{}", socket_addr);

        let (incoming_tx, incoming_rx) = std::sync::mpsc::sync_channel(DEFALT_INCOMING_QUEUE_SIZE);
        let (outgoing_tx, outgoing_rx) =
            mio_extras::channel::sync_channel(DEFALT_OUTGOING_QUEUE_SIZE);
        let (registration, ready_ctrl) = mio::Registration::new2();
        let stream_driver = StreamDriver::new(socket, incoming_tx, ready_ctrl);

        self.reactor_sender
            .send(ReactorCtrl::Connection(stream_driver, outgoing_rx))
            .map_err(|err| {
                ConnectError::GeneralError(format!(
                    "unable to send connection stream to reactor: {}",
                    err
                ))
            })?;

        Ok(Box::new(RawConnection {
            sender: outgoing_tx,
            receiver: incoming_rx,
            evented: RawConnectionEvented { registration },
            local_endpoint,
            remote_endpoint,
        }))
    }

    fn listen(&mut self, bind: &str) -> Result<Box<dyn Listener>, ListenError> {
        if !self.accepts(bind) {
            return Err(ListenError::ProtocolError(format!(
                "Invalid protocol \"{}\"",
                bind
            )));
        }
        let address = if bind.starts_with(PROTOCOL_PREFIX) {
            &bind[PROTOCOL_PREFIX.len()..]
        } else {
            bind
        };

        let socket_addr = address
            .parse()
            .map_err(|err| ListenError::ProtocolError(format!("invalid bind string: {}", err)))?;
        let tcp_listener = TcpListener::bind(&socket_addr)?;

        let endpoint = format!("tcp://{}", tcp_listener.local_addr().unwrap());

        let (accept_sender, accept_receiver) =
            std::sync::mpsc::sync_channel(DEFALT_LISTENER_QUEUE_SIZE);

        self.reactor_sender
            .send(ReactorCtrl::Listener(ListenerDriver {
                tcp_listener,
                accept_sender,
            }))
            .unwrap();

        Ok(Box::new(RawListener {
            accept_receiver,
            endpoint,
        }))
    }
}

struct RawListener {
    accept_receiver: std::sync::mpsc::Receiver<RawConnection>,
    endpoint: String,
}

impl Listener for RawListener {
    fn accept(&mut self) -> Result<Box<dyn Connection>, AcceptError> {
        Ok(Box::new(self.accept_receiver.recv().map_err(|err| {
            AcceptError::GeneralError(format!(
                "Unable to receive connection from reactor: {}",
                err
            ))
        })?))
    }

    fn endpoint(&self) -> String {
        self.endpoint.clone()
    }
}

enum ConnCtrl {
    Msg(Vec<u8>, std::sync::mpsc::Sender<Result<(), SendError>>),
    HangUp,
}

struct RawConnection {
    sender: mio_extras::channel::SyncSender<ConnCtrl>,
    receiver: std::sync::mpsc::Receiver<Result<Vec<u8>, RecvError>>,
    evented: RawConnectionEvented,
    local_endpoint: String,
    remote_endpoint: String,
}

impl Connection for RawConnection {
    fn send(&mut self, message: &[u8]) -> Result<(), SendError> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.sender
            .try_send(ConnCtrl::Msg(message.to_vec(), tx))
            .map_err(|err| match err {
                mio_extras::channel::TrySendError::Io(err) => SendError::IoError(err),
                mio_extras::channel::TrySendError::Full(_) => SendError::QueueFull,
                mio_extras::channel::TrySendError::Disconnected(_) => SendError::Disconnected,
            })?;

        rx.recv().map_err(|_| {
            // the transport probably was shutdown and dropped the sender
            SendError::Disconnected
        })?
    }

    fn recv(&mut self) -> Result<Vec<u8>, RecvError> {
        if let Ok(res) = self.receiver.recv() {
            res
        } else {
            Err(RecvError::Disconnected)
        }
    }

    fn remote_endpoint(&self) -> String {
        self.remote_endpoint.clone()
    }

    fn local_endpoint(&self) -> String {
        self.local_endpoint.clone()
    }

    fn disconnect(&mut self) -> Result<(), DisconnectError> {
        match self.sender.send(ConnCtrl::HangUp) {
            Err(mio_extras::channel::SendError::Io(err)) => Err(DisconnectError::IoError(err)),
            _ => Ok(()),
        }
    }

    fn evented(&self) -> &dyn Evented {
        &self.evented
    }
}

struct RawConnectionEvented {
    registration: mio::Registration,
}

impl Evented for RawConnectionEvented {
    fn register(
        &self,
        poll: &mio::Poll,
        token: mio::Token,
        interest: mio::Ready,
        opts: mio::PollOpt,
    ) -> std::io::Result<()> {
        self.registration.register(poll, token, interest, opts)
    }

    fn reregister(
        &self,
        poll: &mio::Poll,
        token: mio::Token,
        interest: mio::Ready,
        opts: mio::PollOpt,
    ) -> std::io::Result<()> {
        self.registration.reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &mio::Poll) -> std::io::Result<()> {
        poll.deregister(&self.registration)
    }
}

struct ListenerDriver {
    tcp_listener: TcpListener,
    accept_sender: std::sync::mpsc::SyncSender<RawConnection>,
}

struct StreamDriver {
    stream: TcpStream,
    conn_sender: std::sync::mpsc::SyncSender<Result<Vec<u8>, RecvError>>,
    incoming_buf: Option<MsgBuffer>,
    outgoing_buf: Option<MsgBuffer>,
    ready_ctrl: mio::SetReadiness,
}

impl StreamDriver {
    fn new(
        stream: TcpStream,
        conn_sender: std::sync::mpsc::SyncSender<Result<Vec<u8>, RecvError>>,
        ready_ctrl: mio::SetReadiness,
    ) -> Self {
        Self {
            stream,
            conn_sender,
            ready_ctrl,
            incoming_buf: None,
            outgoing_buf: None,
        }
    }
}

struct IncomingStreamDriver {
    receiver: mio_extras::channel::Receiver<ConnCtrl>,
    stream_token: mio::Token,
}

enum EventDriver {
    Control(mio_extras::channel::Receiver<ReactorCtrl>),
    Listener(ListenerDriver),
    Stream(StreamDriver),
    IncomingStream(IncomingStreamDriver),
}

enum ReactorCtrl {
    Connection(StreamDriver, mio_extras::channel::Receiver<ConnCtrl>),
    Listener(ListenerDriver),
    Shutdown,
}

#[derive(Debug)]
struct TcpReactorError(String);

impl std::error::Error for TcpReactorError {}

impl std::fmt::Display for TcpReactorError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "error in tcp reactor: {}", self.0)
    }
}

struct TcpReactor {
    event_drivers: std::collections::HashMap<mio::Token, EventDriver>,
    poll: mio::Poll,
    token_id: usize,
}

impl TcpReactor {
    fn new(
        connect_receiver: mio_extras::channel::Receiver<ReactorCtrl>,
    ) -> Result<Self, TcpReactorError> {
        let poll = mio::Poll::new()
            .map_err(|err| TcpReactorError(format!("Unable to create poll instance: {}", err)))?;

        poll.register(
            &connect_receiver,
            mio::Token(0),
            mio::Ready::readable(),
            mio::PollOpt::edge(),
        )
        .map_err(|err| TcpReactorError(format!("Unable to register control channel: {}", err)))?;

        let mut event_drivers = std::collections::HashMap::new();
        event_drivers.insert(mio::Token(0), EventDriver::Control(connect_receiver));

        Ok(Self {
            event_drivers,
            poll,
            token_id: 1,
        })
    }

    fn run(&mut self) -> Result<(), TcpReactorError> {
        let mut events = mio::Events::with_capacity(1024);
        loop {
            self.poll
                .poll(&mut events, Some(std::time::Duration::from_millis(100)))
                .map_err(|err| TcpReactorError(format!("Unable to poll for events: {}", err)))?;

            for event in &events {
                match self.event_drivers.remove(&event.token()) {
                    Some(EventDriver::Control(control_receiver)) => {
                        eprintln!("controller event: {:?}", event.token());
                        if self.handle_control(event.token(), control_receiver)? {
                            return Ok(());
                        }
                    }
                    Some(EventDriver::Listener(listener)) => {
                        eprintln!("listener event: {:?}", event.token());
                        self.accept_connections(event.token(), listener)?;
                    }
                    Some(EventDriver::Stream(stream)) => self.handle_stream(&event, stream)?,
                    Some(EventDriver::IncomingStream(incoming)) => {
                        self.handle_incoming_stream(event.token(), incoming)?
                    }
                    None => {
                        error!("Received event for unknown token");
                    }
                }
            }
        }
    }

    fn next_token(&mut self) -> mio::Token {
        self.token_id += 1;
        mio::Token(self.token_id)
    }

    fn handle_control(
        &mut self,
        control_token: mio::Token,
        control_receiver: mio_extras::channel::Receiver<ReactorCtrl>,
    ) -> Result<bool, TcpReactorError> {
        loop {
            match control_receiver.try_recv() {
                Ok(ReactorCtrl::Listener(listener_driver)) => {
                    eprintln!(
                        "Registering listener: {}",
                        socket_addr_to_string(listener_driver.tcp_listener.local_addr(), "local")
                    );

                    let token = self.next_token();
                    self.poll
                        .register(
                            &listener_driver.tcp_listener,
                            token,
                            mio::Ready::readable(),
                            mio::PollOpt::edge(),
                        )
                        .map_err(|err| {
                            TcpReactorError(format!("unable to poll new tcp listener: {}", err))
                        })?;
                    self.event_drivers
                        .insert(token, EventDriver::Listener(listener_driver));
                }
                Ok(ReactorCtrl::Connection(stream_driver, incoming_receiver)) => {
                    eprintln!(
                        "Registering connection: {} => {}",
                        socket_addr_to_string(stream_driver.stream.local_addr(), "local"),
                        socket_addr_to_string(stream_driver.stream.peer_addr(), "remote")
                    );
                    let stream_token = self.next_token();
                    self.poll
                        .register(
                            &stream_driver.stream,
                            stream_token,
                            mio::Ready::readable(),
                            mio::PollOpt::edge(),
                        )
                        .map_err(|err| {
                            TcpReactorError(format!("unable to register new connecton: {}", err))
                        })?;
                    let conn_ctrl_token = self.next_token();
                    self.poll
                        .register(
                            &incoming_receiver,
                            conn_ctrl_token,
                            mio::Ready::readable(),
                            mio::PollOpt::edge(),
                        )
                        .map_err(|err| {
                            TcpReactorError(format!(
                                "unable to register new incoming receiver: {}",
                                err
                            ))
                        })?;

                    self.event_drivers
                        .insert(stream_token, EventDriver::Stream(stream_driver));

                    self.event_drivers.insert(
                        conn_ctrl_token,
                        EventDriver::IncomingStream(IncomingStreamDriver {
                            receiver: incoming_receiver,
                            stream_token,
                        }),
                    );
                }
                Ok(ReactorCtrl::Shutdown) => {
                    for (_, event_driver) in self.event_drivers.drain() {
                        match event_driver {
                            EventDriver::Control(control_receiver) => {
                                if let Err(err) = self.poll.deregister(&control_receiver) {
                                    error!("unable to deregister control receiver: {}", err);
                                }
                            }
                            EventDriver::Listener(listener) => {
                                if let Err(err) = self.poll.deregister(&listener.tcp_listener) {
                                    error!("unable to deregister listener: {}", err);
                                }
                            }
                            EventDriver::Stream(stream_driver) => {
                                if let Err(err) = self.poll.deregister(&stream_driver.stream) {
                                    error!("unable to deregister stream: {}", err);
                                }
                            }
                            EventDriver::IncomingStream(incoming_driver) => {
                                if let Err(err) = self.poll.deregister(&incoming_driver.receiver) {
                                    error!("unable to deregister stream: {}", err);
                                }
                            }
                        }
                    }
                    return Ok(true);
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => break,
                Err(_) => break,
            }
        }

        self.event_drivers
            .insert(control_token, EventDriver::Control(control_receiver));

        Ok(false)
    }

    fn accept_connections(
        &mut self,
        listener_token: mio::Token,
        listener_driver: ListenerDriver,
    ) -> Result<(), TcpReactorError> {
        loop {
            match listener_driver.tcp_listener.accept() {
                Ok((socket, address)) => {
                    eprintln!(
                        "Accepting connection: {} <= {}",
                        socket_addr_to_string(socket.local_addr(), "local"),
                        socket_addr_to_string(socket.peer_addr(), "remote")
                    );
                    let stream_token = self.next_token();
                    self.poll
                        .register(
                            &socket,
                            stream_token,
                            mio::Ready::readable(),
                            mio::PollOpt::edge(),
                        )
                        .map_err(|err| {
                            TcpReactorError(format!(
                                "unable to poll new incoming connection: {}",
                                err
                            ))
                        })?;

                    let (tx, rx) = std::sync::mpsc::sync_channel(16);
                    let (incoming_tx, incoming_rx) = mio_extras::channel::sync_channel(16);

                    let conn_ctrl_token = self.next_token();
                    self.poll
                        .register(
                            &incoming_rx,
                            conn_ctrl_token,
                            mio::Ready::readable(),
                            mio::PollOpt::edge(),
                        )
                        .map_err(|err| {
                            TcpReactorError(format!(
                                "unable to poll new incoming connecton: {}",
                                err
                            ))
                        })?;

                    let local_endpoint = format!(
                        "tcp://{}",
                        socket_addr_to_string(socket.local_addr(), "local")
                    );
                    let remote_endpoint = format!("tcp://{}", address);
                    self.event_drivers.insert(
                        conn_ctrl_token,
                        EventDriver::IncomingStream(IncomingStreamDriver {
                            receiver: incoming_rx,
                            stream_token,
                        }),
                    );
                    let (registration, ready_ctrl) = mio::Registration::new2();
                    self.event_drivers.insert(
                        stream_token,
                        EventDriver::Stream(StreamDriver::new(socket, tx, ready_ctrl)),
                    );

                    eprintln!("returning RawConnection to listener");
                    match listener_driver.accept_sender.try_send(RawConnection {
                        sender: incoming_tx,
                        receiver: rx,
                        local_endpoint,
                        remote_endpoint,
                        evented: RawConnectionEvented { registration },
                    }) {
                        Ok(()) => (),
                        Err(std::sync::mpsc::TrySendError::Full(_)) => break,
                        Err(std::sync::mpsc::TrySendError::Disconnected(_)) => break,
                    }
                }
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                    eprintln!("Listener Would block!");
                    break;
                }
                Err(e) => {
                    error!(
                        "Unable to accept connections on {}: {}",
                        socket_addr_to_string(listener_driver.tcp_listener.local_addr(), "local"),
                        e
                    );
                    eprintln!(
                        "Unable to accept connections on {}: {}",
                        socket_addr_to_string(listener_driver.tcp_listener.local_addr(), "local"),
                        e
                    );
                    break;
                }
            }
        }
        // re-insert the listener (this avoids a lexical-lifetime problem)
        self.event_drivers
            .insert(listener_token, EventDriver::Listener(listener_driver));

        Ok(())
    }

    fn handle_stream(
        &mut self,
        event: &mio::Event,
        mut stream_driver: StreamDriver,
    ) -> Result<(), TcpReactorError> {
        if event.readiness().is_writable() {
            if stream_driver.outgoing_buf.is_none() {
                warn!(
                    "Write was requested for connection {}, but had no outgoing message; \
                     returning to readable",
                    event.token().0
                );
                self.poll
                    .reregister(
                        &stream_driver.stream,
                        event.token(),
                        mio::Ready::readable(),
                        mio::PollOpt::edge(),
                    )
                    .map_err(|err| {
                        TcpReactorError(format!(
                            "Unable to restore tcp stream to readable: {}",
                            err
                        ))
                    })?;
            } else {
                loop {
                    if stream_driver.outgoing_buf.as_ref().unwrap().transmitted == 0 {
                        match stream_driver.stream.write_u32::<BigEndian>(
                            stream_driver
                                .outgoing_buf
                                .as_ref()
                                .unwrap()
                                .content_length() as u32,
                        ) {
                            Ok(()) => (),
                            Err(ref err) if err.kind() == ErrorKind::WouldBlock => {
                                self.poll
                                    .reregister(
                                        &stream_driver.stream,
                                        event.token(),
                                        mio::Ready::writable(),
                                        mio::PollOpt::edge() | mio::PollOpt::oneshot(),
                                    )
                                    .map_err(|err| {
                                        TcpReactorError(format!(
                                            "Unable to re-request tcp stream as writable: {}",
                                            err
                                        ))
                                    })?;
                                break;
                            }
                            Err(ref err)
                                if err.kind() == ErrorKind::ConnectionAborted
                                    || err.kind() == ErrorKind::BrokenPipe =>
                            {
                                // the connection disconnected, deregister and remove
                                return self.remove_connection(&stream_driver);
                            }
                            Err(err) => {
                                error!(
                                    "Unable to write content length on connection {}: {}",
                                    event.token().0,
                                    err
                                );
                                break;
                            }
                        }
                    }

                    match stream_driver.stream.write(
                        stream_driver
                            .outgoing_buf
                            .as_mut()
                            .unwrap()
                            .as_byte_buffer(),
                    ) {
                        Ok(n) if n > 0 => {
                            stream_driver
                                .outgoing_buf
                                .as_mut()
                                .unwrap()
                                .update_transmitted(n);
                            if stream_driver.outgoing_buf.as_ref().unwrap().is_complete() {
                                stream_driver.stream.flush().map_err(|err| {
                                    TcpReactorError(format!("Unable to flush tcp stream: {}", err))
                                })?;
                                stream_driver.outgoing_buf = None;

                                self.poll
                                    .reregister(
                                        &stream_driver.stream,
                                        event.token(),
                                        mio::Ready::readable(),
                                        mio::PollOpt::edge(),
                                    )
                                    .map_err(|err| {
                                        TcpReactorError(format!(
                                            "Unable to restore tcp stream to readable: {}",
                                            err
                                        ))
                                    })?;

                                break;
                            }
                        }
                        Ok(_) => {
                            // i.e. n == 0
                            // the connection disconnected, deregister and remove
                            return self.remove_connection(&stream_driver);
                        }
                        Err(ref err) if err.kind() == ErrorKind::WouldBlock => {
                            self.poll
                                .reregister(
                                    &stream_driver.stream,
                                    event.token(),
                                    mio::Ready::writable(),
                                    mio::PollOpt::edge() | mio::PollOpt::oneshot(),
                                )
                                .map_err(|err| {
                                    TcpReactorError(format!(
                                        "Unable to re-request tcp stream as writable: {}",
                                        err
                                    ))
                                })?;
                            break;
                        }

                        Err(ref err)
                            if err.kind() == ErrorKind::ConnectionAborted
                                || err.kind() == ErrorKind::BrokenPipe =>
                        {
                            // the connection disconnected, deregister and remove
                            return self.remove_connection(&stream_driver);
                        }

                        Err(err) => {
                            error!(
                                "Unable to write bytes on connection {}: {}",
                                event.token().0,
                                err
                            );
                            break;
                        }
                    }
                }
            }
        } else if event.readiness().is_readable() {
            loop {
                if stream_driver.incoming_buf.is_none() {
                    let len = match stream_driver.stream.read_u32::<BigEndian>() {
                        Ok(n) => n,
                        Err(ref err) if err.kind() == ErrorKind::WouldBlock => {
                            break;
                        }
                        Err(ref err) if err.kind() == ErrorKind::UnexpectedEof => {
                            // the connection disconnected, deregister and remove
                            return self.remove_connection(&stream_driver);
                        }
                        Err(err) => {
                            error!(
                                "Unable to receive size on connection {}: {}",
                                event.token().0,
                                err
                            );
                            break;
                        }
                    };
                    stream_driver.incoming_buf = Some(MsgBuffer::with_content_length(len as usize));
                }

                match stream_driver.stream.read(
                    stream_driver
                        .incoming_buf
                        .as_mut()
                        .unwrap()
                        .as_byte_buffer(),
                ) {
                    Ok(n) => {
                        if n == 0 {
                            debug!("Connection {} reached EOF; ", event.token().0);
                            return self.remove_connection(&stream_driver);
                        }
                        stream_driver
                            .incoming_buf
                            .as_mut()
                            .unwrap()
                            .update_transmitted(n);
                        if stream_driver.incoming_buf.as_ref().unwrap().is_complete() {
                            let msg = stream_driver.incoming_buf.take().unwrap();
                            match stream_driver.conn_sender.try_send(Ok(msg.take_bytes())) {
                                Ok(()) => {
                                    stream_driver
                                        .ready_ctrl
                                        .set_readiness(mio::Ready::readable())
                                        .map_err(|err| {
                                            TcpReactorError(format!(
                                                "unable to set readiness: {}",
                                                err
                                            ))
                                        })?;
                                }
                                Err(std::sync::mpsc::TrySendError::Full(_)) => {
                                    warn!(
                                        "connection ({},{}) recv queue full, dropping msg",
                                        socket_addr_to_string(
                                            stream_driver.stream.local_addr(),
                                            "local"
                                        ),
                                        socket_addr_to_string(
                                            stream_driver.stream.peer_addr(),
                                            "remote"
                                        ),
                                    );
                                    break;
                                }
                                Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                                    // no one is listening to this connection any more, so we'll
                                    // drop the connection from our set of tokens
                                    self.poll.deregister(&stream_driver.stream).map_err(|err| {
                                        TcpReactorError(format!(
                                            "Unable to deregister tcp stream: {}",
                                            err
                                        ))
                                    })?;
                                    return Ok(());
                                }
                            }
                        }
                    }
                    Err(ref err) if err.kind() == ErrorKind::WouldBlock => break,
                    Err(ref err) if err.kind() == ErrorKind::UnexpectedEof => {
                        return self.remove_connection(&stream_driver);
                    }
                    Err(err) => {
                        error!(
                            "Unable to receive bytes on connection {}: {}",
                            event.token().0,
                            err
                        );
                        break;
                    }
                }
            }
        }

        // re-insert the listener (this avoids a lexical-lifetime problem)
        self.event_drivers
            .insert(event.token(), EventDriver::Stream(stream_driver));

        Ok(())
    }

    fn remove_connection(&mut self, stream_driver: &StreamDriver) -> Result<(), TcpReactorError> {
        self.poll
            .deregister(&stream_driver.stream)
            .map_err(|err| TcpReactorError(format!("Unable to deregister tcp stream: {}", err)))?;

        if let Err(err) = stream_driver.stream.shutdown(std::net::Shutdown::Both) {
            warn!("Unable to close socket: {}", err);
        }

        if let Err(_) = stream_driver
            .conn_sender
            .try_send(Err(RecvError::Disconnected))
        {
            debug!("Unable to send explicit disconnect message");
        }
        Ok(())
    }

    fn handle_incoming_stream(
        &mut self,
        incoming_stream_token: mio::Token,
        incoming_stream_driver: IncomingStreamDriver,
    ) -> Result<(), TcpReactorError> {
        let stream_driver = match self
            .event_drivers
            .get_mut(&incoming_stream_driver.stream_token)
        {
            Some(EventDriver::Stream(stream_driver)) => stream_driver,
            Some(_) => panic!("stream was internally misconfigured!"),
            None => {
                debug!("Stream was dropped, dropping incoming stream as well");
                self.poll
                    .deregister(&incoming_stream_driver.receiver)
                    .map_err(|err| {
                        TcpReactorError(format!(
                            "Unable to deregister incoming stream channel: {}",
                            err
                        ))
                    })?;
                return Ok(());
            }
        };

        match incoming_stream_driver.receiver.try_recv() {
            Ok(ConnCtrl::Msg(message_bytes, response_sender)) => {
                if stream_driver.outgoing_buf.is_none() {
                    stream_driver.outgoing_buf = Some(MsgBuffer::from_bytes(message_bytes));
                    self.poll
                        .reregister(
                            &stream_driver.stream,
                            incoming_stream_driver.stream_token,
                            mio::Ready::writable(),
                            mio::PollOpt::edge() | mio::PollOpt::oneshot(),
                        )
                        .map_err(|err| {
                            TcpReactorError(format!(
                                "Unable to re-request tcp stream as writable: {}",
                                err
                            ))
                        })?;
                    if let Err(_) = response_sender.send(Ok(())) {
                        error!("connection some how was dropped mid-send-response receive");
                    }
                } else {
                    // should be a back-pressure error
                    if let Err(_) = response_sender.send(Err(SendError::WouldBlock)) {
                        error!("connection some how was dropped mid-send-response receive");
                    }
                }
            }
            Ok(ConnCtrl::HangUp) => {
                self.poll
                    .deregister(&incoming_stream_driver.receiver)
                    .map_err(|err| {
                        TcpReactorError(format!(
                            "Unable to deregister incoming stream channel: {}",
                            err
                        ))
                    })?;
                self.poll.deregister(&stream_driver.stream).map_err(|err| {
                    TcpReactorError(format!("Unable to deregister stream : {}", err))
                })?;
                if let Err(err) = stream_driver.stream.shutdown(std::net::Shutdown::Both) {
                    warn!("Unable to close socket: {}", err);
                }
                return Ok(());
            }
            Err(std::sync::mpsc::TryRecvError::Empty) => (),
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                // no one is listening anymore - the connection has been dropped by the caller
                self.poll
                    .deregister(&incoming_stream_driver.receiver)
                    .map_err(|err| {
                        TcpReactorError(format!(
                            "Unable to deregister incoming stream channel: {}",
                            err
                        ))
                    })?;

                // cleanup the connection
                self.poll.deregister(&stream_driver.stream).map_err(|err| {
                    TcpReactorError(format!("Unable to deregister stream : {}", err))
                })?;
                if let Err(err) = stream_driver.stream.shutdown(std::net::Shutdown::Both) {
                    warn!("Unable to close socket: {}", err);
                }
                self.event_drivers
                    .remove(&incoming_stream_driver.stream_token);
                return Ok(());
            }
        }

        self.event_drivers.insert(
            incoming_stream_token,
            EventDriver::IncomingStream(incoming_stream_driver),
        );

        Ok(())
    }
}

fn socket_addr_to_string(
    addr: std::io::Result<std::net::SocketAddr>,
    type_str: &'static str,
) -> String {
    addr.map(|addr| addr.to_string())
        .unwrap_or_else(|_| format!("<unknown {} addr>", type_str))
}

struct MsgBuffer {
    buffer: Vec<u8>,
    transmitted: usize,
}

impl MsgBuffer {
    fn with_content_length(len: usize) -> Self {
        Self {
            buffer: vec![0; len],
            transmitted: 0,
        }
    }

    fn from_bytes(bytes: Vec<u8>) -> Self {
        Self {
            buffer: bytes,
            transmitted: 0,
        }
    }

    fn content_length(&self) -> usize {
        self.buffer.len()
    }

    fn as_byte_buffer(&mut self) -> &mut [u8] {
        &mut self.buffer[self.transmitted..]
    }

    fn take_bytes(self) -> Vec<u8> {
        self.buffer
    }

    fn update_transmitted(&mut self, transmitted: usize) {
        self.transmitted += transmitted
    }

    fn is_complete(&self) -> bool {
        self.buffer.len() == self.transmitted
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::tests;
    use mio::Ready;

    #[test]
    fn test_accepts() {
        let transport = RawTransport::new().expect("Unable to create transport");
        assert!(transport.accepts("127.0.0.1:0"));
        assert!(transport.accepts("tcp://127.0.0.1:0"));
        assert!(transport.accepts("tcp://somewhere.example.com:4000"));

        assert!(!transport.accepts("tls://somewhere.example.com:4000"));
    }

    #[test]
    fn test_transport() {
        let transport = RawTransport::new().expect("Unable to create transport");

        tests::test_transport(transport, "127.0.0.1:0");
    }

    #[test]
    fn test_transport_scaling() {
        let transport = RawTransport::new().expect("Unable to create transport");

        tests::test_transport_parameterized(transport, "127.0.0.1:0", 10, 10, 1024);
    }

    #[test]
    fn test_transport_explicit_protocol() {
        let transport = RawTransport::new().expect("Unable to create transport");

        tests::test_transport(transport, "tcp://127.0.0.1:0");
    }

    #[test]
    fn test_transport_remote_connection_closed() {
        let transport = RawTransport::new().expect("Unable to create transport");

        tests::test_transport_remote_connection_closed(transport, "127.0.0.1:0");
    }

    #[test]
    fn test_transport_local_connection_closed() {
        let transport = RawTransport::new().expect("Unable to create transport");

        tests::test_transport_local_connection_closed(transport, "127.0.0.1:0");
    }

    /*
    #[test]
    fn test_transport_queue_full() {
        let transport = RawTransport::new().expect("Unable to create transport");

        tests::test_transport_queue_full(transport, "127.0.0.1:0", DEFALT_OUTGOING_QUEUE_SIZE);
    }
    */

    #[cfg(feature = "large-message-tests")]
    #[test]
    fn test_async_transport_large_msg() {
        let transport = RawTransport::new().expect("Unable to create transport");

        tests::test_transport_parameterized(transport, "127.0.0.1:0", 4, 10, 16 * 1024 * 1024);
    }

    #[test]
    fn test_poll() {
        let transport = RawTransport::new().expect("Unable to create transport");
        tests::test_poll(transport, "127.0.0.1:0", Ready::readable());
    }
}
