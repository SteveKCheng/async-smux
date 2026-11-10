use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};

/// The window a v2 sender assumes before the receiver's first window
/// update, as in the golang implementation. It is also the default
/// [`MuxConfig::max_stream_buffer`].
pub const INITIAL_PEER_WINDOW: u32 = 256 * 1024;

/// The smux protocol spoken on the carrier. smux has no version
/// negotiation, so both ends must be configured alike; a frame with a
/// different version byte is a protocol error that closes the session.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ProtocolVersion {
    /// SYN, FIN, PSH and NOP. There is no per-stream flow control: when
    /// the session-wide receive budget ([`MuxConfig::max_rx_queue`]) runs
    /// out, the carrier stops being read, and every stream stalls.
    #[default]
    V1,
    /// Protocol version 2, from golang's smux library.  Version 2
    /// supplements Version 1 with per-stream flow control.  A stream
    /// whose reader stops only stalls its own writer.
    /// The receiver sends a new frame type, UPD, to inform the sender
    /// of its current receive window.
    V2,
}

impl ProtocolVersion {
    /// The version byte at the start of every frame.
    pub(crate) fn wire_byte(self) -> u8 {
        match self {
            ProtocolVersion::V1 => 1,
            ProtocolVersion::V2 => 2,
        }
    }

    /// Whether streams are flow-controlled with UPD frames.
    pub(crate) fn has_flow_control(self) -> bool {
        match self {
            ProtocolVersion::V1 => false,
            ProtocolVersion::V2 => true,
        }
    }
}

/// Parity used for locally-allocated stream ids. Server uses `Even`
/// (0, 2, 4, …) and client uses `Odd` (1, 3, 5, …) so the two sides
/// never collide. Stream id 0 is reserved for NOP keep-alive frames
/// and is never handed out as a real stream id.
#[derive(Clone, Copy, Debug)]
pub enum StreamIdType {
    Even = 0,
    Odd = 1,
}

/// Session configuration. Construct via [`crate::MuxBuilder`] for the
/// fluent API, or build one directly and pass it to
/// [`crate::mux_connection`].
#[derive(Clone, Copy, Debug)]
pub struct MuxConfig {
    /// Protocol version. See [`ProtocolVersion`].
    pub version: ProtocolVersion,
    /// Parity for locally-allocated stream ids. See [`StreamIdType`].
    pub stream_id_type: StreamIdType,
    /// If set, send a NOP frame this often (seconds) and enable
    /// dead-peer detection.
    pub keep_alive_interval: Option<NonZeroU64>,
    /// Maximum gap (seconds) between any two received frames before
    /// the peer is declared dead and the connection closed. Only
    /// consulted when `keep_alive_interval` is also set. Defaults to
    /// 3 × `keep_alive_interval`.
    pub keep_alive_timeout: Option<NonZeroU64>,
    /// If set, close any stream that sees no traffic for this many
    /// seconds.
    pub idle_timeout: Option<NonZeroU64>,
    /// Backpressure threshold for outbound frames per stream.
    pub max_tx_queue: NonZeroUsize,
    /// Backpressure threshold for inbound frames and unaccepted streams
    /// across the session. Dead-peer detection is suspended while this
    /// budget is exhausted because the carrier is intentionally not polled.
    ///
    /// With [`ProtocolVersion::V2`], stream data doesn't count: each
    /// stream's window bounds it instead. Only unaccepted streams do.
    pub max_rx_queue: NonZeroUsize,
    /// Receive window of each stream, in bytes, with
    /// [`ProtocolVersion::V2`]. A window update is sent each time the
    /// reader has consumed half of it. Values above `i32::MAX` are
    /// clamped, since the golang implementation treats the window as signed.
    pub max_stream_buffer: NonZeroU32,
}
