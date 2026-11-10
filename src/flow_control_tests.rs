//! Tests of protocol version 2: per-stream flow control with UPD frames.

use std::{
    num::NonZeroU32,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio_util::codec::{Decoder, Encoder};

use crate::{
    config::INITIAL_PEER_WINDOW,
    error::MuxError,
    frame::{MuxCodec, MuxCommand, MuxFrame},
    MuxAcceptor, MuxBuilder, MuxConnector, ProtocolVersion,
};

const SYN: u8 = 0;
const PSH: u8 = 2;
const UPD: u8 = 4;

type Mux = (MuxConnector<DuplexStream>, MuxAcceptor<DuplexStream>);

/// A client and a server speaking v2 over an in-memory carrier, both with
/// the given window.
fn v2_pair(window: u32) -> (Mux, Mux) {
    let (a, b) = tokio::io::duplex(4096);
    let window = NonZeroU32::new(window).unwrap();
    let (connector_a, acceptor_a, worker_a) = MuxBuilder::client()
        .with_version(ProtocolVersion::V2)
        .with_max_stream_buffer(window)
        .with_connection(a)
        .build();
    let (connector_b, acceptor_b, worker_b) = MuxBuilder::server()
        .with_version(ProtocolVersion::V2)
        .with_max_stream_buffer(window)
        .with_connection(b)
        .build();
    tokio::spawn(worker_a);
    tokio::spawn(worker_b);
    ((connector_a, acceptor_a), (connector_b, acceptor_b))
}

/// A v2 client whose peer is a raw carrier the test speaks frames on.
fn v2_with_raw_peer(window: u32) -> (Mux, DuplexStream) {
    let (a, peer) = tokio::io::duplex(1 << 20);
    let (connector, acceptor, worker) = MuxBuilder::client()
        .with_version(ProtocolVersion::V2)
        .with_max_stream_buffer(NonZeroU32::new(window).unwrap())
        .with_connection(a)
        .build();
    tokio::spawn(worker);
    ((connector, acceptor), peer)
}

fn raw_v2_frame(command: u8, stream_id: u32, payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(8 + payload.len());
    frame.push(2);
    frame.push(command);
    frame.extend_from_slice(&(payload.len() as u16).to_le_bytes());
    frame.extend_from_slice(&stream_id.to_le_bytes());
    frame.extend_from_slice(payload);
    frame
}

fn update_payload(consumed: u32, window: u32) -> Vec<u8> {
    let mut payload = consumed.to_le_bytes().to_vec();
    payload.extend_from_slice(&window.to_le_bytes());
    payload
}

/// Read one raw frame: (version, command, stream id, payload).
async fn read_raw_frame(peer: &mut DuplexStream) -> (u8, u8, u32, Vec<u8>) {
    let mut header = [0u8; 8];
    peer.read_exact(&mut header).await.unwrap();
    let len = u16::from_le_bytes([header[2], header[3]]) as usize;
    let stream_id = u32::from_le_bytes(header[4..8].try_into().unwrap());
    let mut payload = vec![0; len];
    peer.read_exact(&mut payload).await.unwrap();
    (header[0], header[1], stream_id, payload)
}

/// Read raw frames until a UPD arrives, and return (consumed, window).
async fn next_update(peer: &mut DuplexStream) -> (u32, u32) {
    loop {
        let (version, command, _, payload) = read_raw_frame(peer).await;
        assert_eq!(version, 2);
        if command == UPD {
            let consumed = u32::from_le_bytes(payload[..4].try_into().unwrap());
            let window = u32::from_le_bytes(payload[4..].try_into().unwrap());
            return (consumed, window);
        }
    }
}

/// Wait until the session has closed the carrier, so the raw peer reads
/// end-of-file.
async fn expect_carrier_closed(peer: &mut DuplexStream) {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut sink = [0u8; 4096];
        while peer.read(&mut sink).await.unwrap_or(0) != 0 {}
    })
    .await
    .expect("session did not close the carrier");
}

fn pattern(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

#[tokio::test]
async fn stalled_stream_does_not_block_others() {
    const LEN: usize = 1 << 20;
    let ((connector_a, _acceptor_a), (_connector_b, mut acceptor_b)) = v2_pair(64 * 1024);

    let mut blocked = connector_a.connect().unwrap();
    let mut blocked_peer = acceptor_b.accept().await.unwrap();
    let mut live = connector_a.connect().unwrap();
    let mut live_peer = acceptor_b.accept().await.unwrap();

    let written = Arc::new(AtomicUsize::new(0));
    let data = pattern(1, LEN);
    let writer = tokio::spawn({
        let written = written.clone();
        let data = data.clone();
        async move {
            let mut offset = 0;
            while offset < data.len() {
                offset += blocked.write(&data[offset..]).await.unwrap();
                written.store(offset, Ordering::SeqCst);
            }
            blocked.flush().await.unwrap();
            blocked
        }
    });

    // Nobody reads `blocked_peer`, so no window update comes back, and the
    // writer stops at the window it assumes initially.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!writer.is_finished());
    assert_eq!(written.load(Ordering::SeqCst), INITIAL_PEER_WINDOW as usize);

    // The other stream still works both ways. Under v1 the carrier would
    // stall as soon as the receive budget filled up.
    tokio::time::timeout(Duration::from_secs(5), async {
        live.write_all(b"ping").await.unwrap();
        live.flush().await.unwrap();
        let mut buf = [0u8; 4];
        live_peer.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");
        live_peer.write_all(b"pong").await.unwrap();
        live_peer.flush().await.unwrap();
        live.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"pong");
    })
    .await
    .expect("an unrelated stream stalled");

    // Reading the stalled stream opens its window again.
    let mut received = vec![0; LEN];
    tokio::time::timeout(
        Duration::from_secs(10),
        blocked_peer.read_exact(&mut received),
    )
    .await
    .expect("reader did not reopen the window")
    .unwrap();
    assert!(received == data);
    tokio::time::timeout(Duration::from_secs(5), writer)
        .await
        .expect("writer did not finish")
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_transfer_with_small_windows() {
    const STREAMS: u8 = 4;
    const LEN: usize = 2 << 20;
    // Small windows need many window updates; a lost or miscounted one
    // deadlocks the transfer.
    let ((connector_a, _acceptor_a), (_connector_b, mut acceptor_b)) = v2_pair(8 * 1024);

    tokio::time::timeout(Duration::from_secs(30), async {
        let mut tasks = tokio::task::JoinSet::new();
        for index in 0..STREAMS {
            let seed = index;
            let (mut local_read, mut local_write) =
                tokio::io::split(connector_a.connect().unwrap());
            let (mut peer_read, mut peer_write) =
                tokio::io::split(acceptor_b.accept().await.unwrap());
            tasks.spawn(async move {
                local_write.write_all(&pattern(seed, LEN)).await.unwrap();
                local_write.shutdown().await.unwrap();
            });
            tasks.spawn(async move {
                peer_write
                    .write_all(&pattern(seed.wrapping_add(100), LEN))
                    .await
                    .unwrap();
                peer_write.shutdown().await.unwrap();
            });
            tasks.spawn(async move {
                let mut buf = Vec::new();
                peer_read.read_to_end(&mut buf).await.unwrap();
                assert!(buf == pattern(seed, LEN));
            });
            tasks.spawn(async move {
                let mut buf = Vec::new();
                local_read.read_to_end(&mut buf).await.unwrap();
                assert!(buf == pattern(seed.wrapping_add(100), LEN));
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    })
    .await
    .expect("transfer deadlocked");
}

#[tokio::test]
async fn window_updates_follow_reads() {
    const WINDOW: u32 = 64 * 1024;
    let ((_connector, mut acceptor), mut peer) = v2_with_raw_peer(WINDOW);
    let stream_id = 2;
    peer.write_all(&raw_v2_frame(SYN, stream_id, &[]))
        .await
        .unwrap();
    peer.write_all(&raw_v2_frame(PSH, stream_id, &[7; 10]))
        .await
        .unwrap();
    let mut stream = acceptor.accept().await.unwrap();

    // The first read announces our window.
    let mut buf = vec![0; 64 * 1024];
    assert_eq!(stream.read(&mut buf).await.unwrap(), 10);
    assert_eq!(next_update(&mut peer).await, (10, WINDOW));

    // The next one waits until half the window has been consumed.
    let chunk = vec![7; 1024];
    let mut sent = 10;
    for _ in 0..(WINDOW / 2 / 1024) {
        peer.write_all(&raw_v2_frame(PSH, stream_id, &chunk))
            .await
            .unwrap();
        sent += 1024;
    }
    let mut read = 10;
    while read < sent {
        read += stream.read(&mut buf).await.unwrap();
    }
    assert_eq!(next_update(&mut peer).await, (sent as u32, WINDOW));
}

#[tokio::test]
async fn writer_waits_for_window_update() {
    let ((connector, _acceptor), mut peer) = v2_with_raw_peer(INITIAL_PEER_WINDOW);
    let mut stream = connector.connect().unwrap();
    let stream_id = stream.get_stream_id();

    // A window smaller than the initial guess limits the writer at once.
    peer.write_all(&raw_v2_frame(UPD, stream_id, &update_payload(0, 1000)))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let written = Arc::new(AtomicUsize::new(0));
    let writer = tokio::spawn({
        let written = written.clone();
        async move {
            let data = [1u8; 3000];
            let mut offset = 0;
            while offset < data.len() {
                offset += stream.write(&data[offset..]).await.unwrap();
                written.store(offset, Ordering::SeqCst);
            }
            stream.flush().await.unwrap();
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(written.load(Ordering::SeqCst), 1000);

    // Each update lets exactly the acknowledged amount through.
    peer.write_all(&raw_v2_frame(UPD, stream_id, &update_payload(1000, 1000)))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(written.load(Ordering::SeqCst), 2000);

    peer.write_all(&raw_v2_frame(UPD, stream_id, &update_payload(1500, 2000)))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), writer)
        .await
        .expect("writer was not woken")
        .unwrap();

    // Frames never exceeded the window either.
    let mut total = 0;
    while total < 3000 {
        let (_, command, _, payload) = read_raw_frame(&mut peer).await;
        if command == PSH {
            total += payload.len();
        }
    }
    assert_eq!(total, 3000);
}

#[tokio::test]
async fn peer_overrunning_window_closes_session() {
    let ((_connector, _acceptor), mut peer) = v2_with_raw_peer(64 * 1024);
    let stream_id = 2;
    let (mut peer_read, mut peer_write) = tokio::io::split(&mut peer);
    let write = async {
        // Nobody accepts or reads the stream, so the peer may send only
        // the initial 256 KiB.
        let _ = peer_write
            .write_all(&raw_v2_frame(SYN, stream_id, &[]))
            .await;
        let chunk = vec![0; 0x8000];
        for _ in 0..(INITIAL_PEER_WINDOW as usize / chunk.len()) {
            let _ = peer_write
                .write_all(&raw_v2_frame(PSH, stream_id, &chunk))
                .await;
        }
        let _ = peer_write
            .write_all(&raw_v2_frame(PSH, stream_id, b"!"))
            .await;
    };
    let read = async {
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut sink = [0u8; 4096];
            while peer_read.read(&mut sink).await.unwrap_or(0) != 0 {}
        })
        .await
        .expect("session did not close the carrier");
    };
    tokio::join!(write, read);
}

#[tokio::test]
async fn update_acknowledging_unsent_data_closes_session() {
    let ((connector, _acceptor), mut peer) = v2_with_raw_peer(INITIAL_PEER_WINDOW);
    let stream = connector.connect().unwrap();
    peer.write_all(&raw_v2_frame(
        UPD,
        stream.get_stream_id(),
        &update_payload(1, INITIAL_PEER_WINDOW),
    ))
    .await
    .unwrap();
    expect_carrier_closed(&mut peer).await;
}

#[tokio::test]
async fn update_for_unknown_stream_is_ignored() {
    let ((connector, _acceptor), mut peer) = v2_with_raw_peer(INITIAL_PEER_WINDOW);
    peer.write_all(&raw_v2_frame(UPD, 99, &update_payload(5, 5)))
        .await
        .unwrap();
    let mut stream = connector.connect().unwrap();
    stream.write_all(b"still alive").await.unwrap();
    stream.flush().await.unwrap();
    loop {
        let (_, command, _, payload) = read_raw_frame(&mut peer).await;
        if command == PSH {
            assert_eq!(payload, b"still alive");
            break;
        }
    }
}

#[test]
fn codec_round_trips_update() {
    let mut codec = MuxCodec::new(ProtocolVersion::V2);
    let mut wire = BytesMut::new();
    codec
        .encode(MuxFrame::new_update(3, 0xdead_beef, 12345), &mut wire)
        .unwrap();
    assert_eq!(
        &wire[..],
        &raw_v2_frame(UPD, 3, &update_payload(0xdead_beef, 12345))[..]
    );
    let frame = codec.decode(&mut wire).unwrap().unwrap();
    assert_eq!(frame.header.command, MuxCommand::Update);
    assert_eq!(frame.update_fields(), (0xdead_beef, 12345));
}

#[test]
fn codec_v1_rejects_update() {
    let mut codec = MuxCodec::new(ProtocolVersion::V1);
    let mut frame = raw_v2_frame(UPD, 3, &update_payload(0, 0));
    frame[0] = 1;
    let mut wire = BytesMut::from(&frame[..]);
    assert!(matches!(
        codec.decode(&mut wire),
        Err(MuxError::InvalidCommand(4))
    ));
}

#[test]
fn codec_rejects_malformed_update() {
    let mut codec = MuxCodec::new(ProtocolVersion::V2);
    let mut short = BytesMut::from(&raw_v2_frame(UPD, 3, &[0; 4])[..]);
    assert!(matches!(
        codec.decode(&mut short),
        Err(MuxError::InvalidUpdatePayload(4))
    ));
    let mut on_zero = BytesMut::from(&raw_v2_frame(UPD, 0, &update_payload(0, 0))[..]);
    assert!(matches!(
        codec.decode(&mut on_zero),
        Err(MuxError::ReservedStreamId(0))
    ));
}

#[test]
fn codec_rejects_other_version() {
    let mut v2 = MuxCodec::new(ProtocolVersion::V2);
    let mut frame = raw_v2_frame(PSH, 3, b"x");
    frame[0] = 1;
    assert!(matches!(
        v2.decode(&mut BytesMut::from(&frame[..])),
        Err(MuxError::InvalidVersion(1))
    ));
    let mut v1 = MuxCodec::new(ProtocolVersion::V1);
    assert!(matches!(
        v1.decode(&mut BytesMut::from(&raw_v2_frame(PSH, 3, b"x")[..])),
        Err(MuxError::InvalidVersion(2))
    ));
}
