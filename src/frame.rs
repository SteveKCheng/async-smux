use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use std::io::Cursor;

use crate::{
    config::ProtocolVersion,
    error::{MuxError, MuxResult},
};

pub const HEADER_SIZE: usize = 8;
pub const MAX_PAYLOAD_SIZE: usize = 0xffff;
/// Payload size of a v2 UPD frame: consumed bytes, then window size.
pub const UPD_PAYLOAD_SIZE: usize = 8;

#[derive(Eq, PartialEq, Debug, Clone, Copy)]
pub(crate) enum MuxCommand {
    Sync = 0,
    Finish = 1,
    Push = 2,
    Nop = 3,
    /// Window update (v2): the receiver reports how many bytes it has
    /// consumed and how large its window is.
    Update = 4,
}

impl TryFrom<u8> for MuxCommand {
    type Error = MuxError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(MuxCommand::Sync),
            1 => Ok(MuxCommand::Finish),
            2 => Ok(MuxCommand::Push),
            3 => Ok(MuxCommand::Nop),
            4 => Ok(MuxCommand::Update),
            _ => Err(MuxError::InvalidCommand(value)),
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) struct MuxFrameHeader {
    pub command: MuxCommand,
    pub length: u16,
    pub stream_id: u32,
}

impl MuxFrameHeader {
    #[inline]
    fn encode(&self, version: ProtocolVersion, buf: &mut BytesMut) {
        buf.put_u8(version.wire_byte());
        buf.put_u8(self.command as u8);
        buf.put_u16_le(self.length);
        buf.put_u32_le(self.stream_id);
    }

    #[inline]
    fn decode(version: ProtocolVersion, buf: &[u8]) -> MuxResult<Self> {
        let mut cursor = Cursor::new(buf);
        let wire_version = cursor.get_u8();
        if wire_version != version.wire_byte() {
            return Err(MuxError::InvalidVersion(wire_version));
        }
        let command = MuxCommand::try_from(cursor.get_u8())?;
        if command == MuxCommand::Update && !version.has_flow_control() {
            return Err(MuxError::InvalidCommand(command as u8));
        }
        let length = cursor.get_u16_le();
        let stream_id = cursor.get_u32_le();
        Ok(Self {
            command,
            length,
            stream_id,
        })
    }
}

#[derive(Clone)]
pub(crate) struct MuxFrame {
    pub header: MuxFrameHeader,
    pub payload: Bytes,
}

impl MuxFrame {
    pub fn new(command: MuxCommand, stream_id: u32, payload: Bytes) -> Self {
        assert!(payload.len() <= MAX_PAYLOAD_SIZE);
        Self {
            header: MuxFrameHeader {
                command,
                length: payload.len() as u16,
                stream_id,
            },
            payload,
        }
    }

    /// A v2 window update: `consumed` is the total number of bytes read
    /// from the stream so far (modulo 2^32), `window` the receive window.
    pub fn new_update(stream_id: u32, consumed: u32, window: u32) -> Self {
        let mut payload = BytesMut::with_capacity(UPD_PAYLOAD_SIZE);
        payload.put_u32_le(consumed);
        payload.put_u32_le(window);
        Self::new(MuxCommand::Update, stream_id, payload.freeze())
    }

    /// The (consumed, window) pair of a UPD frame. The codec has already
    /// checked the payload size.
    pub fn update_fields(&self) -> (u32, u32) {
        debug_assert_eq!(self.header.command, MuxCommand::Update);
        let mut payload = &self.payload[..];
        (payload.get_u32_le(), payload.get_u32_le())
    }
}

/// Encodes and decodes frames of one protocol version. smux has no version
/// negotiation: both ends must be configured alike, and a frame stamped
/// with another version is a protocol error.
pub(crate) struct MuxCodec {
    version: ProtocolVersion,
}

impl MuxCodec {
    pub fn new(version: ProtocolVersion) -> Self {
        Self { version }
    }
}

impl Decoder for MuxCodec {
    type Item = MuxFrame;
    type Error = MuxError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < HEADER_SIZE {
            src.reserve(HEADER_SIZE - src.len());
            return Ok(None);
        }
        let header = MuxFrameHeader::decode(self.version, src)?;
        // Per smux v1, only PSH carries payload; SYN/FIN/NOP must be empty.
        // A v2 UPD carries exactly its two counters.
        // These checks depend only on the header, so reject immediately rather
        // than waiting for an invalid peer to deliver its claimed payload.
        match header.command {
            MuxCommand::Sync | MuxCommand::Finish | MuxCommand::Nop if header.length != 0 => {
                return Err(MuxError::InvalidControlFramePayload(header.length));
            }
            MuxCommand::Update if header.length as usize != UPD_PAYLOAD_SIZE => {
                return Err(MuxError::InvalidUpdatePayload(header.length));
            }
            _ => {}
        }
        match header.command {
            MuxCommand::Nop if header.stream_id != 0 => {
                return Err(MuxError::InvalidNopStreamId(header.stream_id));
            }
            MuxCommand::Sync | MuxCommand::Finish | MuxCommand::Push | MuxCommand::Update
                if header.stream_id == 0 =>
            {
                return Err(MuxError::ReservedStreamId(0));
            }
            _ => {}
        }

        let len = header.length as usize;
        let frame_len = HEADER_SIZE + len;
        if src.len() < frame_len {
            src.reserve(frame_len - src.len());
            return Ok(None);
        }
        src.advance(HEADER_SIZE);
        let payload = src.split_to(len).freeze();

        debug_assert!(payload.len() == len);
        let frame = MuxFrame { header, payload };

        Ok(Some(frame))
    }
}

impl Encoder<MuxFrame> for MuxCodec {
    type Error = MuxError;

    fn encode(&mut self, item: MuxFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        if item.payload.len() > MAX_PAYLOAD_SIZE {
            return Err(MuxError::PayloadTooLarge(item.payload.len()));
        }

        item.header.encode(self.version, dst);
        dst.put_slice(&item.payload);

        Ok(())
    }
}
