//! The crate's error type.

use crate::endpoint::EndpointError;
use std::{fmt, io};

/// Everything that can stop a tsflow operation.
///
/// Damaged input never produces an error: it is skipped and counted (see
/// [`FramerStats`](crate::framer::FramerStats) and
/// [`Summary`](crate::monitor::Summary)). Errors are reserved for things the
/// caller has to act on: a malformed endpoint, a socket that cannot be
/// opened, a file that cannot be read, or settings that conflict.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// An endpoint string could not be parsed, or does not suit the
    /// direction it was used in.
    Endpoint(EndpointError),
    /// An I/O operation failed. `context` names what was being done.
    Io {
        /// What was being done, for example "open in.ts".
        context: String,
        /// The underlying error.
        source: io::Error,
    },
    /// A setting is out of range, or two settings conflict.
    Config(String),
    /// KLV injection stopped: the injector refused the stream or a unit,
    /// or the metadata source could not be read or encoded.
    Inject(String),
}

impl Error {
    pub(crate) fn io(context: impl Into<String>, source: io::Error) -> Error {
        Error::Io { context: context.into(), source }
    }

    pub(crate) fn config(message: impl Into<String>) -> Error {
        Error::Config(message.into())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Endpoint(e) => e.fmt(f),
            Error::Io { context, source } => write!(f, "{context}: {source}"),
            Error::Config(message) => f.write_str(message),
            Error::Inject(message) => write!(f, "inject: {message}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Endpoint(e) => Some(e),
            Error::Io { source, .. } => Some(source),
            Error::Config(_) | Error::Inject(_) => None,
        }
    }
}

impl From<EndpointError> for Error {
    fn from(e: EndpointError) -> Error {
        Error::Endpoint(e)
    }
}

/// `Result` with [`Error`] as the default error type.
pub type Result<T, E = Error> = std::result::Result<T, E>;
