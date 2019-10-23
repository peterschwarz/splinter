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

use std::error::Error;
use std::fmt;
use std::io;

macro_rules! impl_from_io_error {
    ($err:ident) => {
        impl From<io::Error> for $err {
            fn from(io_error: io::Error) -> Self {
                $err::IoError(io_error)
            }
        }
    };
}

macro_rules! impl_from_io_error_ext {
    ($err:ident) => {
        impl From<io::Error> for $err {
            fn from(io_error: io::Error) -> Self {
                match io_error.kind() {
                    io::ErrorKind::UnexpectedEof => $err::Disconnected,
                    io::ErrorKind::WouldBlock => $err::WouldBlock,
                    _ => $err::IoError(io_error),
                }
            }
        }
    };
}

#[derive(Debug)]
pub enum SendError {
    IoError(io::Error),
    ProtocolError(String),
    WouldBlock,
    Disconnected,
}

impl Error for SendError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            SendError::IoError(ref e) => Some(e),
            _ => None,
        }
    }
}

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SendError::IoError(e) => write!(f, "an I/O error occurred while sending: {}", e),
            SendError::ProtocolError(msg) => {
                write!(f, "a protocol error occurred while sending: {}", msg)
            }
            SendError::WouldBlock => f.write_str(
                "the requested operation would block, but the connection is in non-blocking mode",
            ),
            SendError::Disconnected => f.write_str("the connection was disconnected"),
        }
    }
}

impl_from_io_error_ext!(SendError);

#[derive(Debug)]
pub enum RecvError {
    IoError(io::Error),
    ProtocolError(String),
    WouldBlock,
    Disconnected,
}

impl Error for RecvError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            RecvError::IoError(ref e) => Some(e),
            _ => None,
        }
    }
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RecvError::IoError(e) => write!(f, "an I/O error occurred while receiving: {}", e),
            RecvError::ProtocolError(msg) => {
                write!(f, "a protocol error occurred while receiving: {}", msg)
            }
            RecvError::WouldBlock => f.write_str(
                "the requested operation would block, but the connection is in non-blocking mode",
            ),
            RecvError::Disconnected => f.write_str("the connection was disconnected"),
        }
    }
}

impl_from_io_error_ext!(RecvError);

#[derive(Debug)]
pub enum DisconnectError {
    IoError(io::Error),
    ProtocolError(String),
}

impl Error for DisconnectError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            DisconnectError::IoError(ref e) => Some(e),
            _ => None,
        }
    }
}

impl fmt::Display for DisconnectError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DisconnectError::IoError(e) => {
                write!(f, "an I/O error occurred while disconnecting: {}", e)
            }
            DisconnectError::ProtocolError(msg) => {
                write!(f, "a protocol error occurred while disconnecting: {}", msg)
            }
        }
    }
}

impl_from_io_error!(DisconnectError);

#[derive(Debug)]
pub enum AcceptError {
    IoError(io::Error),
    ProtocolError(String),
    GeneralError(String),
}

impl Error for AcceptError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            AcceptError::IoError(ref e) => Some(e),
            _ => None,
        }
    }
}

impl fmt::Display for AcceptError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AcceptError::IoError(e) => write!(f, "an I/O error occurred while accepting: {}", e),
            AcceptError::ProtocolError(msg) => {
                write!(f, "a protocol error occurred while accepting: {}", msg)
            }
            AcceptError::GeneralError(msg) => {
                write!(f, "an error occurred while accepting: {}", msg)
            }
        }
    }
}

impl_from_io_error!(AcceptError);

#[derive(Debug)]
pub enum ConnectError {
    IoError(io::Error),
    ParseError(String),
    ProtocolError(String),
    GeneralError(String),
}

impl Error for ConnectError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ConnectError::IoError(err) => Some(err),
            ConnectError::ParseError(_) => None,
            ConnectError::ProtocolError(_) => None,
            ConnectError::GeneralError(_) => None,
        }
    }
}

impl fmt::Display for ConnectError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ConnectError::IoError(err) => write!(f, "io error occurred: {}", err),
            ConnectError::ParseError(err) => write!(f, "error while parsing: {}", err),
            ConnectError::ProtocolError(err) => write!(f, "protocol error occurred: {}", err),
            ConnectError::GeneralError(msg) => {
                write!(f, "error occurred during connection: {}", msg)
            }
        }
    }
}

impl_from_io_error!(ConnectError);

#[derive(Debug)]
pub enum ListenError {
    IoError(io::Error),
    ProtocolError(String),
}

impl Error for ListenError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ListenError::IoError(ref e) => Some(e),
            _ => None,
        }
    }
}

impl fmt::Display for ListenError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ListenError::IoError(e) => write!(f, "an I/O error occurred while listening: {}", e),
            ListenError::ProtocolError(msg) => {
                write!(f, "a protocol error occurred while listening: {}", msg)
            }
        }
    }
}

impl_from_io_error!(ListenError);