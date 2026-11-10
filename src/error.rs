use thiserror::Error;

use crate::config::StreamIdType;

#[derive(Debug, Error)]
pub enum MuxError {
    #[error("Invalid command {0}")]
    InvalidCommand(u8),
    #[error("Invalid version {0}")]
    InvalidVersion(u8),
    #[error("Payload too large {0}")]
    PayloadTooLarge(usize),
    #[error("Control frame must have empty payload, got length {0}")]
    InvalidControlFramePayload(u16),
    #[error("UPD frame must carry 8 bytes, got length {0}")]
    InvalidUpdatePayload(u16),
    #[error("RST and STOP frames must carry 8 bytes, got length {0}")]
    InvalidErrorCodePayload(u16),
    #[error("Reserved stream id {0}")]
    ReservedStreamId(u32),
    #[error("NOP frame must use stream id 0, got {0}")]
    InvalidNopStreamId(u32),
    #[error("Duplicated stream id {0}")]
    DuplicatedStreamId(u32),

    #[error("Invalid stream ID from peer: {0}, local stream ID type: {1:?}")]
    InvalidPeerStreamIdType(u32, StreamIdType),

    #[error("Peer acknowledged more data than was sent on stream {0}")]
    InvalidWindowUpdate(u32),
    #[error("Peer overran the receive window of stream {0}")]
    WindowExceeded(u32),

    #[error("Too many streams")]
    TooManyStreams,

    #[error("Inner connection closed")]
    ConnectionClosed,
    #[error("Mux stream closed: {0:x}")]
    StreamClosed(u32),
    /// The peer abandoned its direction of the stream with RST. Reads
    /// report it as `io::ErrorKind::ConnectionReset`.
    #[error("Stream reset by peer with error code {0}")]
    StreamReset(u64),
    /// The peer asked us, with STOP, to stop sending on the stream. Writes
    /// report it as `io::ErrorKind::BrokenPipe`.
    #[error("Peer stopped reading the stream, with error code {0}")]
    StopSending(u64),
    /// The session's protocol version can't do this: aborting streams
    /// needs `ProtocolVersion::V2WithReset`.
    #[error("Not supported by the session's protocol version")]
    Unsupported,

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

impl MuxError {
    /// The `MuxError` inside an `io::Error` returned by a stream's
    /// `AsyncRead` or `AsyncWrite` methods, if any. Use it to get at the
    /// error code of [`MuxError::StreamReset`] or [`MuxError::StopSending`].
    pub fn from_io(error: &std::io::Error) -> Option<&MuxError> {
        error.get_ref()?.downcast_ref()
    }
}

pub type MuxResult<T> = Result<T, MuxError>;
