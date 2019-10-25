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

pub mod error;
pub mod inproc;
pub mod multi;
pub mod raw;
mod rw;
pub mod tls;
#[cfg(feature = "zmq-transport")]
pub mod zmq;

use mio::Evented;

pub use self::error::{
    AcceptError, ConnectError, DisconnectError, ListenError, RecvError, SendError,
};

pub enum Status {
    Connected,
    Disconnected,
}

/// A bi-directional connection between two nodes
pub trait Connection: Send {
    /// Attempt to send a message consisting of bytes across the connection.
    fn send(&mut self, message: &[u8]) -> Result<(), SendError>;

    /// Attempt to receive a message consisting of bytes from the connection.
    fn recv(&mut self) -> Result<Vec<u8>, RecvError>;

    /// Return the remote endpoint address for this connection.
    ///
    /// For TCP-based connection types, this will contain the remote peer
    /// socket address.
    fn remote_endpoint(&self) -> String;

    /// Return the local endpoint address for this connection.
    ///
    /// For TCP-based connection types, this will contain the local
    /// socket address.
    fn local_endpoint(&self) -> String;

    /// Shut down the connection.
    ///
    /// After the connection has been disconnected, messages cannot be sent
    /// or received.
    fn disconnect(&mut self) -> Result<(), DisconnectError>;

    /// Returns a `mio::event::Evented` for this connection which can be used for polling.
    fn evented(&self) -> &dyn Evented;
}

pub trait Listener: Send {
    fn accept(&mut self) -> Result<Box<dyn Connection>, AcceptError>;
    fn endpoint(&self) -> String;
}

pub trait Incoming {
    fn incoming<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = Result<Box<dyn Connection>, AcceptError>> + 'a>;
}

impl Incoming for dyn Listener {
    fn incoming<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = Result<Box<dyn Connection>, AcceptError>> + 'a> {
        Box::new(IncomingIter::new(self))
    }
}

/// Factory-pattern based type for creating connections
pub trait Transport {
    /// Indicates whether or not a given address can be used to create a conneciton or listener.
    fn accepts(&self, address: &str) -> bool;
    fn connect(&mut self, endpoint: &str) -> Result<Box<dyn Connection>, ConnectError>;
    fn listen(&mut self, bind: &str) -> Result<Box<dyn Listener>, ListenError>;
}

// Helper struct for extending Listener to Incoming

struct IncomingIter<'a> {
    listener: &'a mut dyn Listener,
}

impl<'a> IncomingIter<'a> {
    pub fn new(listener: &'a mut dyn Listener) -> Self {
        IncomingIter { listener }
    }
}

impl<'a> Iterator for IncomingIter<'a> {
    type Item = Result<Box<dyn Connection>, AcceptError>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.listener.accept())
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use std::fmt::Debug;

    use std::sync::mpsc::channel;
    use std::thread;
    use std::time::Duration;

    use mio::{Events, Poll, PollOpt, Ready, Token};

    fn assert_ok<T, E: Debug>(result: Result<T, E>) -> T {
        match result {
            Ok(ok) => ok,
            Err(err) => panic!("Expected Ok(...), got Err({:?})", err),
        }
    }

    macro_rules! assert_match {
        ($expected:pat, $source:expr) => {{
            let res = $source;
            match res {
                $expected => (),
                _ => {
                    assert!(
                        false,
                        "Expected {}, but was {:?}",
                        stringify!($expected),
                        res
                    );
                }
            }
        }};
    }

    pub fn test_transport<T: Transport + Send + 'static>(transport: T, bind: &str) {
        test_transport_parameterized(transport, bind, 1, 1, 3);
    }

    pub fn test_transport_parameterized<T: Transport + Send + 'static>(
        mut transport: T,
        bind: &str,
        connections: usize,
        message_count: usize,
        message_size: usize,
    ) {
        let mut listener = transport.listen(bind).expect("Unable to create listener");
        let endpoint = listener.endpoint();

        let client_handle = thread::spawn(move || {
            let mut handles = vec![];
            for _ in 0..connections {
                let mut client = transport
                    .connect(&endpoint)
                    .expect("Unable to connect to server");
                assert_eq!(client.remote_endpoint(), endpoint);
                let handle = thread::spawn(move || {
                    for _ in 0..message_count {
                        let msg = (0..10).cycle().take(message_size).collect::<Vec<u8>>();
                        blocking_send(client.as_mut(), &msg);
                    }
                    assert_eq!(b"hangup".to_vec(), blocking_recv(client.as_mut()));
                });
                handles.push(handle);
            }
            for handle in handles.into_iter() {
                handle
                    .join()
                    .expect("unable to join individual client thread");
            }
        });

        let server_conns: Result<Vec<Box<dyn Connection>>, AcceptError> =
            listener.incoming().take(connections).collect();

        let mut server_conns = server_conns.expect("Unable to receive connections from clients");

        let expected_msg = (0..10).cycle().take(message_size).collect::<Vec<u8>>();

        for server_conn in server_conns.iter_mut() {
            for _ in 0..message_count {
                let received = blocking_recv(server_conn.as_mut());
                assert!(
                    expected_msg == received,
                    "Expected did not match received; expected {} byte(s), was {}",
                    message_size,
                    received.len()
                );
            }
            blocking_send(server_conn.as_mut(), b"hangup");
        }

        client_handle.join().expect("Unable to join client thread");
    }

    /// Test that connections from a given transport behave correctly when the remote end (i.e. the
    /// user of "connect") of the connection has closed.
    ///
    /// Approach:
    ///
    /// 1. Create a listener by calling Transport::listen and verify the creation was successful
    /// 2. Create a child thread closing over the transport
    /// 3. The child thread creates a single connection by calling Transport::connect
    /// 4. The child thread will send a message to the parent
    /// 5. The child thread calls Connection::disconnect
    /// 6. The parent thread accepts the connection via Listener::incoming
    /// 7. The parent thread receives the message sent by the child thread
    /// 8. The parent thread should attempt receive again, but receive a RecvError::Disconnected
    /// 8. The parent thread should attempt to send again but receive a SendError::Disconnected
    pub fn test_transport_remote_connection_closed<T: Transport + Send + 'static>(
        mut transport: T,
        bind: &str,
    ) {
        let mut listener = transport.listen(bind).expect("Unable to create listener");
        let endpoint = listener.endpoint();

        let client_handle = thread::spawn(move || {
            let mut client = transport
                .connect(&endpoint)
                .expect("Unable to connect to server");

            assert_eq!(endpoint, client.remote_endpoint());
            blocking_send(client.as_mut(), b"client msg");

            client.disconnect().expect("Could not disconnect client");
        });

        let mut server_conn = listener
            .incoming()
            .next()
            .expect("No client connection received") // i.e. should not be None
            .expect("Unable to receive connections"); // i.e should be Ok

        assert_eq!(b"client msg".to_vec(), blocking_recv(server_conn.as_mut()));

        client_handle.join().expect("Unable to join client thread");

        assert_match!(Err(RecvError::Disconnected), server_conn.recv());
        assert_match!(
            Err(SendError::Disconnected),
            server_conn.send(b"server msg")
        );
    }

    /// Test that connections from a given transport behave correctly when the local end (i.e. the
    /// user of listener::accept) of the connection has closed.
    ///
    /// Approach:
    ///
    /// 1. Create a listener by calling Transport::listen and verify the creation was successful
    /// 2. Create a child thread closing over the transport
    /// 3. The parent thread accepts the connection via Listener::incoming
    /// 4. The parent thread sends message sent to the child thread
    /// 5. The parent thread calls Connection::disconnect
    /// 6. The child thread creates a single connection by calling Transport::connect
    /// 7. The child thread receives the message sent from the parent thread
    /// 8. The child thread should attempt receive again, but receive a RecvError::Disconnected
    /// 9. The child thread should attempt to send again but receive a SendError::Disconnected
    pub fn test_transport_local_connection_closed<T: Transport + Send + 'static>(
        mut transport: T,
        bind: &str,
    ) {
        let mut listener = transport.listen(bind).expect("Unable to create listener");
        let endpoint = listener.endpoint();

        let client_handle = thread::spawn(move || {
            let mut client = transport
                .connect(&endpoint)
                .expect("Unable to connect to server");

            assert_eq!(endpoint, client.remote_endpoint());
            assert_eq!(b"server msg".to_vec(), blocking_recv(client.as_mut()));

            assert_match!(Err(RecvError::Disconnected), client.recv());
            assert_match!(Err(SendError::Disconnected), client.send(b"client msg"));
        });

        let mut server_conn = listener
            .incoming()
            .next()
            .expect("No client connection received") // i.e. should not be None
            .expect("Unable to receive connections"); // i.e should be Ok

        blocking_send(server_conn.as_mut(), b"server msg");

        server_conn
            .disconnect()
            .expect("Could not disconnect server");

        client_handle.join().expect("Unable to join client thread");
    }

    /// Test that connections generate "queue full" errors in the appropriate situations
    ///
    /// Approach:
    ///
    /// 1. Create a listener by calling Transport::listen and verify the creation was successful
    /// 2. Create a child thread closing over the transport
    /// 3. The child thread creates a single connection by calling Transport::connect
    /// 4. The parent thread accepts the connection via Listener::incoming
    /// 5. The parent thread sends QUEUE_LIMIT + 1 messages to the child thread
    /// 6. The last message should resulting in a SendError::QueueFull error result
    /// 7. The child thread should receive QUEUE_LIMIT messages
    pub fn test_transport_queue_full<T: Transport + Send + 'static>(
        mut transport: T,
        bind: &str,
        queue_limit: usize,
    ) {
        let mut listener = transport.listen(bind).expect("Unable to create listener");
        let endpoint = listener.endpoint();

        let (ready_tx, ready_rx) = channel();

        let client_handle = thread::spawn(move || {
            let mut client = transport
                .connect(&endpoint)
                .expect("Unable to connect to server");

            assert_eq!(endpoint, client.remote_endpoint());

            eprintln!("Waiting for server to fill queue");
            ready_rx
                .recv()
                .expect("Server thread did not signal it had finished its activity");
            eprintln!("Queue is full; draining...");

            for _ in 0..queue_limit {
                assert_eq!(b"server_msg".to_vec(), blocking_recv(client.as_mut()));
            }

            eprintln!("Signaling done...");
            blocking_send(client.as_mut(), b"client_done");
        });

        let mut server_conn = listener
            .incoming()
            .next()
            .expect("No client connection received") // i.e. should not be None
            .expect("Unable to receive connections"); // i.e should be Ok

        for _ in 0..queue_limit {
            blocking_send(server_conn.as_mut(), b"server_msg");
        }

        assert_match!(
            Err(SendError::QueueFull),
            server_conn.send(b"queue full msg")
        );

        ready_tx
            .send(1u8)
            .expect("Unable to signal client that queue full was reached");

        assert_eq!(b"client_done".to_vec(), blocking_recv(server_conn.as_mut()));

        client_handle.join().expect("Unable to join client thread");
    }

    fn blocking_recv(conn: &mut dyn Connection) -> Vec<u8> {
        loop {
            match conn.recv() {
                Ok(msg) => break msg,
                Err(RecvError::WouldBlock) => {
                    thread::sleep(Duration::from_millis(100));
                }
                Err(err) => panic!("Unable to recv message due to {:?}", err),
            }
        }
    }

    fn blocking_send(conn: &mut dyn Connection, msg: &[u8]) {
        loop {
            match conn.send(msg) {
                Ok(_) => break,
                // Ultimately, this should return a QueueFull (or other
                // back-pressure error)
                Err(SendError::WouldBlock) => {
                    thread::sleep(Duration::from_millis(100));
                }
                Err(err) => panic!("Unable to send message due to {:?}", err),
            }
        }
    }

    fn assert_ready(events: &Events, token: Token, readiness: Ready) {
        assert_eq!(
            Some(readiness),
            events
                .iter()
                .filter(|event| event.token() == token)
                .map(|event| event.readiness())
                .next(),
        );
    }

    pub fn test_poll<T: Transport + Send + 'static>(
        mut transport: T,
        bind: &str,
        expected_readiness: Ready,
    ) {
        // Create aconnections and register them with the poller
        const CONNECTIONS: usize = 16;

        let mut listener = assert_ok(transport.listen(bind));
        let endpoint = listener.endpoint();

        let (ready_tx, ready_rx) = channel();

        let handle = thread::spawn(move || {
            let mut connections = Vec::with_capacity(CONNECTIONS);
            for i in 0..CONNECTIONS {
                connections.push((assert_ok(transport.connect(&endpoint)), Token(i)));
            }

            // Register all connections with Poller
            let poll = Poll::new().unwrap();
            for (conn, token) in &connections {
                assert_ok(poll.register(
                    conn.evented(),
                    *token,
                    expected_readiness,
                    PollOpt::level(),
                ));
            }

            // Block waiting for other thread to send everything
            ready_rx.recv().unwrap();

            let mut events = Events::with_capacity(CONNECTIONS * 2);
            assert_ok(poll.poll(&mut events, None));
            for (mut conn, token) in connections {
                assert_ready(&events, token, expected_readiness);
                assert_eq!(b"hello".to_vec(), assert_ok(conn.recv()));
                assert_ok(conn.send(b"world"));
            }
        });

        let mut connections = Vec::with_capacity(CONNECTIONS);
        for _ in 0..CONNECTIONS {
            let mut conn = assert_ok(listener.accept());
            blocking_send(conn.as_mut(), b"hello");
            connections.push(conn);
        }

        // Signal done sending to background thread
        ready_tx.send(()).unwrap();

        for mut conn in connections {
            assert_eq!(b"world".to_vec(), blocking_recv(conn.as_mut()));
        }

        handle.join().unwrap();
    }
}
