//! Tests of protocol version 255: aborting streams with RST and STOP.

use std::{
    io::ErrorKind,
    num::{NonZeroU64, NonZeroUsize},
    time::Duration,
};

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio_util::codec::{Decoder, Encoder};

use crate::{
    error::MuxError,
    frame::{MuxCodec, MuxCommand, MuxFrame},
    MuxAcceptor, MuxBuilder, MuxConnector, MuxStream, ProtocolVersion,
};

const PSH: u8 = 2;
const RST: u8 = 5;
const STOP: u8 = 6;

type Mux = (MuxConnector<DuplexStream>, MuxAcceptor<DuplexStream>);

/// A client and a server speaking version 255. `reset_on_drop` applies to
/// the client only.
fn session_pair(reset_on_drop: bool) -> (Mux, Mux) {
    let (a, b) = tokio::io::duplex(1 << 16);
    let (connector_a, acceptor_a, worker_a) = MuxBuilder::client()
        .with_version(ProtocolVersion::V2WithReset)
        .with_reset_on_drop(reset_on_drop)
        .with_connection(a)
        .build();
    let (connector_b, acceptor_b, worker_b) = MuxBuilder::server()
        .with_version(ProtocolVersion::V2WithReset)
        .with_connection(b)
        .build();
    tokio::spawn(worker_a);
    tokio::spawn(worker_b);
    ((connector_a, acceptor_a), (connector_b, acceptor_b))
}

/// A connected pair of streams, client side first. The sessions stay
/// alive as long as the returned connectors.
async fn stream_pair(
    reset_on_drop: bool,
) -> (MuxStream<DuplexStream>, MuxStream<DuplexStream>, (Mux, Mux)) {
    let ((connector_a, acceptor_a), (connector_b, mut acceptor_b)) = session_pair(reset_on_drop);
    let a = connector_a.connect().unwrap();
    let b = acceptor_b.accept().await.unwrap();
    (a, b, ((connector_a, acceptor_a), (connector_b, acceptor_b)))
}

/// A version-255 client whose peer is a raw carrier.
fn with_raw_peer() -> (Mux, DuplexStream) {
    let (a, peer) = tokio::io::duplex(1 << 20);
    let (connector, acceptor, worker) = MuxBuilder::client()
        .with_version(ProtocolVersion::V2WithReset)
        .with_connection(a)
        .build();
    tokio::spawn(worker);
    ((connector, acceptor), peer)
}

fn raw_frame(command: u8, stream_id: u32, payload: &[u8]) -> Vec<u8> {
    let mut frame = vec![255, command];
    frame.extend_from_slice(&(payload.len() as u16).to_le_bytes());
    frame.extend_from_slice(&stream_id.to_le_bytes());
    frame.extend_from_slice(payload);
    frame
}

/// Read raw frames until one with `command` arrives; return its stream id
/// and payload.
async fn next_frame(peer: &mut DuplexStream, command: u8) -> (u32, Vec<u8>) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let mut header = [0u8; 8];
            peer.read_exact(&mut header).await.unwrap();
            assert_eq!(header[0], 255);
            let len = u16::from_le_bytes([header[2], header[3]]) as usize;
            let stream_id = u32::from_le_bytes(header[4..8].try_into().unwrap());
            let mut payload = vec![0; len];
            peer.read_exact(&mut payload).await.unwrap();
            if header[1] == command {
                return (stream_id, payload);
            }
        }
    })
    .await
    .expect("expected frame never came")
}

/// The error kind and error code that an `io::Error` from a stream carries.
fn code_of(error: &std::io::Error) -> (ErrorKind, Option<u64>) {
    let code = match MuxError::from_io(error) {
        Some(MuxError::StreamReset(code)) | Some(MuxError::StopSending(code)) => Some(*code),
        _ => None,
    };
    (error.kind(), code)
}

/// Read until the stream fails, and return the error.
async fn read_error(stream: &mut MuxStream<DuplexStream>) -> std::io::Error {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut buf = vec![0u8; 4096];
        loop {
            match stream.read(&mut buf).await {
                Ok(0) => panic!("read ended cleanly instead of failing"),
                Ok(_) => continue,
                Err(e) => return e,
            }
        }
    })
    .await
    .expect("read never failed")
}

/// Write until the stream fails, and return the error.
async fn write_error(stream: &mut MuxStream<DuplexStream>) -> std::io::Error {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Err(e) = stream.write_all(&[0u8; 1024]).await {
                return e;
            }
            if let Err(e) = stream.flush().await {
                return e;
            }
        }
    })
    .await
    .expect("write never failed")
}

async fn expect_eof(stream: &mut MuxStream<DuplexStream>) -> Vec<u8> {
    let mut buf = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut buf))
        .await
        .expect("read never ended")
        .expect("read failed instead of ending");
    buf
}

#[tokio::test]
async fn reset_discards_data_and_fails_the_peers_read() {
    let (mut a, mut b, _sessions) = stream_pair(false).await;
    a.write_all(b"never read").await.unwrap();
    a.flush().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    a.reset(42).unwrap();

    let error = read_error(&mut b).await;
    assert_eq!(code_of(&error), (ErrorKind::ConnectionReset, Some(42)));

    // The other direction carries on.
    b.write_all(b"still here").await.unwrap();
    b.shutdown().await.unwrap();
    assert_eq!(expect_eof(&mut a).await, b"still here");

    // Writes after our own reset fail.
    assert_eq!(
        a.write(b"x").await.unwrap_err().kind(),
        ErrorKind::BrokenPipe
    );
}

#[tokio::test]
async fn reset_after_shutdown_is_refused() {
    let (mut a, _b, _sessions) = stream_pair(false).await;
    a.shutdown().await.unwrap();
    assert!(matches!(a.reset(1), Err(MuxError::StreamClosed(_))));
}

#[tokio::test]
async fn stop_fails_the_peers_write_and_keeps_the_other_direction() {
    let (mut a, mut b, _sessions) = stream_pair(false).await;
    b.stop_reading(7).unwrap();

    let error = write_error(&mut a).await;
    assert_eq!(code_of(&error), (ErrorKind::BrokenPipe, Some(7)));
    // Reads after STOP end, like reads after shutdown(SHUT_RD).
    assert_eq!(expect_eof(&mut b).await, b"");

    // The direction that wasn't stopped carries on.
    b.write_all(b"reply").await.unwrap();
    b.shutdown().await.unwrap();
    assert_eq!(expect_eof(&mut a).await, b"reply");
}

#[tokio::test]
async fn stop_wakes_a_writer_waiting_for_window() {
    let (mut a, mut b, _sessions) = stream_pair(false).await;
    let writer = tokio::spawn(async move {
        // Far more than the 256 KiB window; nobody reads it.
        let result = a.write_all(&vec![0u8; 4 << 20]).await;
        (a, result)
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!writer.is_finished());

    b.stop_reading(9).unwrap();
    let (_a, result) = tokio::time::timeout(Duration::from_secs(5), writer)
        .await
        .expect("writer was not woken")
        .unwrap();
    assert_eq!(
        code_of(&result.unwrap_err()),
        (ErrorKind::BrokenPipe, Some(9))
    );
}

#[tokio::test]
async fn abort_ends_both_directions() {
    let (mut a, mut b, _sessions) = stream_pair(false).await;
    a.abort(3).unwrap();
    let error = read_error(&mut b).await;
    assert_eq!(code_of(&error), (ErrorKind::ConnectionReset, Some(3)));
    let error = write_error(&mut b).await;
    assert_eq!(code_of(&error), (ErrorKind::BrokenPipe, Some(3)));
}

#[tokio::test]
async fn stop_is_answered_with_reset_echoing_its_code() {
    let ((connector, _acceptor), mut peer) = with_raw_peer();
    let stream = connector.connect().unwrap();
    let stream_id = stream.get_stream_id();
    peer.write_all(&raw_frame(STOP, stream_id, &77u64.to_le_bytes()))
        .await
        .unwrap();
    let (id, payload) = next_frame(&mut peer, RST).await;
    assert_eq!(id, stream_id);
    assert_eq!(payload, 77u64.to_le_bytes());
}

#[tokio::test]
async fn data_for_an_unknown_stream_is_answered_with_stop() {
    let ((_connector, _acceptor), mut peer) = with_raw_peer();
    // Stream 2 was never opened.
    peer.write_all(&raw_frame(PSH, 2, b"hello")).await.unwrap();
    let (id, payload) = next_frame(&mut peer, STOP).await;
    assert_eq!(id, 2);
    assert_eq!(payload, 0u64.to_le_bytes());
}

#[tokio::test]
async fn dropping_a_stream_stops_the_peer_and_finishes_by_default() {
    let (mut a, mut b, _sessions) = stream_pair(false).await;
    a.write_all(b"delivered").await.unwrap();
    drop(a);
    // Without reset-on-drop, what was written still arrives, then a FIN.
    assert_eq!(expect_eof(&mut b).await, b"delivered");
    // The peer was still sending, so it is told to stop.
    let error = write_error(&mut b).await;
    assert_eq!(code_of(&error), (ErrorKind::BrokenPipe, Some(0)));
}

#[tokio::test]
async fn dropping_a_stream_resets_it_with_reset_on_drop() {
    let (mut a, mut b, _sessions) = stream_pair(true).await;
    a.write_all(b"abandoned").await.unwrap();
    drop(a);
    let error = read_error(&mut b).await;
    assert_eq!(code_of(&error), (ErrorKind::ConnectionReset, Some(0)));
}

#[tokio::test]
async fn shutdown_before_drop_finishes_with_reset_on_drop() {
    let (mut a, mut b, _sessions) = stream_pair(true).await;
    a.write_all(b"complete").await.unwrap();
    a.shutdown().await.unwrap();
    drop(a);
    assert_eq!(expect_eof(&mut b).await, b"complete");
}

#[tokio::test]
async fn dropping_the_read_half_stops_the_peer() {
    let (a, mut b, _sessions) = stream_pair(false).await;
    let (read, mut write) = a.into_split();
    drop(read);
    let error = write_error(&mut b).await;
    assert_eq!(code_of(&error), (ErrorKind::BrokenPipe, Some(0)));

    write.write_all(b"from the write half").await.unwrap();
    write.shutdown().await.unwrap();
    assert_eq!(expect_eof(&mut b).await, b"from the write half");
}

#[tokio::test]
async fn dropping_the_write_half_resets_it_with_reset_on_drop() {
    let (a, mut b, _sessions) = stream_pair(true).await;
    let (mut read, write) = a.into_split();
    drop(write);
    let error = read_error(&mut b).await;
    assert_eq!(code_of(&error), (ErrorKind::ConnectionReset, Some(0)));

    // The read half is unaffected.
    b.write_all(b"to the read half").await.unwrap();
    b.shutdown().await.unwrap();
    let mut buf = Vec::new();
    read.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, b"to the read half");
}

#[tokio::test]
async fn streams_refused_by_a_dropped_acceptor_are_aborted() {
    let ((connector_a, _acceptor_a), (_connector_b, acceptor_b)) = session_pair(false);
    drop(acceptor_b);
    let mut a = connector_a.connect().unwrap();
    let error = write_error(&mut a).await;
    assert_eq!(code_of(&error), (ErrorKind::BrokenPipe, Some(0)));
    let error = read_error(&mut a).await;
    assert_eq!(code_of(&error), (ErrorKind::ConnectionReset, Some(0)));
}

#[tokio::test]
async fn unaccepted_streams_are_aborted_when_the_acceptor_goes() {
    let ((connector_a, _acceptor_a), (_connector_b, acceptor_b)) = session_pair(false);
    let mut a = connector_a.connect().unwrap();
    a.write_all(b"knock knock").await.unwrap();
    a.flush().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(acceptor_b);
    let error = read_error(&mut a).await;
    assert_eq!(code_of(&error), (ErrorKind::ConnectionReset, Some(0)));
}

#[tokio::test]
async fn idle_streams_are_aborted() {
    let (a, b) = tokio::io::duplex(1 << 16);
    let (connector_a, _acceptor_a, worker_a) = MuxBuilder::client()
        .with_version(ProtocolVersion::V2WithReset)
        .with_connection(a)
        .build();
    let (_connector_b, mut acceptor_b, worker_b) = MuxBuilder::server()
        .with_version(ProtocolVersion::V2WithReset)
        .with_idle_timeout(NonZeroU64::new(1).unwrap())
        .with_max_rx_queue(NonZeroUsize::new(16).unwrap())
        .with_connection(b)
        .build();
    tokio::spawn(worker_a);
    tokio::spawn(worker_b);
    let mut a = connector_a.connect().unwrap();
    let _b = acceptor_b.accept().await.unwrap();

    let error = read_error(&mut a).await;
    assert_eq!(code_of(&error), (ErrorKind::ConnectionReset, Some(0)));
}

#[tokio::test]
async fn aborting_needs_version_255() {
    let (a, b) = tokio::io::duplex(4096);
    let (connector, _acceptor_a, worker_a) = MuxBuilder::client()
        .with_version(ProtocolVersion::V2)
        .with_connection(a)
        .build();
    let (_connector_b, _acceptor_b, worker_b) = MuxBuilder::server()
        .with_version(ProtocolVersion::V2)
        .with_connection(b)
        .build();
    tokio::spawn(worker_a);
    tokio::spawn(worker_b);
    let mut stream = connector.connect().unwrap();
    assert!(matches!(stream.reset(1), Err(MuxError::Unsupported)));
    assert!(matches!(stream.stop_reading(1), Err(MuxError::Unsupported)));
    assert!(matches!(stream.abort(1), Err(MuxError::Unsupported)));
}

#[test]
fn codec_round_trips_reset_and_stop() {
    let mut codec = MuxCodec::new(ProtocolVersion::V2WithReset);
    for (command, raw) in [(MuxCommand::Reset, RST), (MuxCommand::StopSending, STOP)] {
        let mut wire = BytesMut::new();
        codec
            .encode(
                MuxFrame::new_with_error_code(command, 3, u64::MAX - 1),
                &mut wire,
            )
            .unwrap();
        assert_eq!(
            &wire[..],
            &raw_frame(raw, 3, &(u64::MAX - 1).to_le_bytes())[..]
        );
        let frame = codec.decode(&mut wire).unwrap().unwrap();
        assert_eq!(frame.header.command, command);
        assert_eq!(frame.error_code(), u64::MAX - 1);
    }
}

#[test]
fn codec_rejects_malformed_reset_and_stop() {
    let mut codec = MuxCodec::new(ProtocolVersion::V2WithReset);
    for raw in [RST, STOP] {
        let mut short = BytesMut::from(&raw_frame(raw, 3, &[0; 4])[..]);
        assert!(matches!(
            codec.decode(&mut short),
            Err(MuxError::InvalidErrorCodePayload(4))
        ));
        let mut on_zero = BytesMut::from(&raw_frame(raw, 0, &[0; 8])[..]);
        assert!(matches!(
            codec.decode(&mut on_zero),
            Err(MuxError::ReservedStreamId(0))
        ));
    }
}

#[test]
fn codec_v2_rejects_reset_and_stop() {
    let mut codec = MuxCodec::new(ProtocolVersion::V2);
    for raw in [RST, STOP] {
        let mut frame = raw_frame(raw, 3, &[0; 8]);
        frame[0] = 2;
        assert!(matches!(
            codec.decode(&mut BytesMut::from(&frame[..])),
            Err(MuxError::InvalidCommand(c)) if c == raw
        ));
    }
}
