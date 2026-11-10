use std::num::{NonZeroU64, NonZeroUsize};

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
}

impl ProtocolVersion {
    /// The version byte at the start of every frame.
    pub(crate) fn wire_byte(self) -> u8 {
        match self {
            ProtocolVersion::V1 => 1,
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
    pub max_rx_queue: NonZeroUsize,
}
