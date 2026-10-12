//! Protocol errors for Kafka wire format parsing and encoding.

use thiserror::Error;

use crate::ApiKey;

/// Protocol parsing, validation, or framing error.
#[derive(Debug, Error)]
pub enum ProtocolError {
    /// Underlying I/O failure.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Frame size exceeds configured maximum.
    #[error("Frame size {size} bytes exceeds maximum limit of {limit} bytes")]
    FrameTooLarge {
        /// Actual parsed frame size.
        size: usize,
        /// Maximum allowed frame size.
        limit: usize,
    },

    /// Invalid or negative frame length prefix.
    #[error("Invalid negative frame length: {0}")]
    InvalidFrameSize(i32),

    /// Buffer exhausted before expected bytes could be read.
    #[error("Unexpected EOF: required {expected} bytes, available {available}")]
    UnexpectedEof {
        /// Expected byte length.
        expected: usize,
        /// Available remaining bytes.
        available: usize,
    },

    /// Corrupted or invalid string length header.
    #[error("Invalid string length: {0}")]
    InvalidStringLength(i32),

    /// String payload is not valid UTF-8.
    #[error("UTF-8 decoding failure: {0}")]
    Utf8Error(#[from] std::string::FromUtf8Error),

    /// Unrecognized API key integer.
    #[error("Unknown or unsupported API key: {0}")]
    UnsupportedApiKey(i16),

    /// Unsupported version for the specified API key.
    #[error("Unsupported version {version} for API key {api_key:?}")]
    UnsupportedVersion {
        /// The API key.
        api_key: ApiKey,
        /// The unsupported version.
        version: i16,
    },

    /// General structural or protocol parsing failure.
    #[error("Invalid protocol format: {0}")]
    InvalidProtocol(String),
}
