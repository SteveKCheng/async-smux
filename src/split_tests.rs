//! Tests of `MuxStream::into_split`: the owned read and write halves.

use std::{num::NonZeroUsize, time::Duration};

use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

use crate::{MuxBuilder, MuxConnector, MuxStream};

/// A connected pair of streams, client side first, with the client's
/// connector (for counting its streams). The server's receive budget is
/// small, so that unread data would stall the session quickly.
async fn stream_pair() -> (
    MuxConnector<DuplexStream>,
    MuxStream<DuplexStream>,
    MuxStream<DuplexStream>,
) {
    let (a, b) = tokio::io::duplex(4096);
    let (connector_a, _acceptor_a, worker_a) = MuxBuilder::client()
        .with_max_rx_queue(NonZeroUsize::new(4).unwrap())
        .with_connection(a)
        .build();
    let (_connector_b, mut acceptor_b, worker_b) = MuxBuilder::server().with_connection(b).build();
    tokio::spawn(worker_a);
    tokio::spawn(worker_b);
    let client = connector_a.connect().unwrap();
    let server = acceptor_b.accept().await.unwrap();
    (connector_a, client, server)
}

#[tokio::test]
async fn halves_work_from_separate_tasks() {
    let (_connector, client, mut server) = stream_pair().await;
    let (mut read, mut write) = client.into_split();
    assert_eq!(read.get_stream_id(), write.get_stream_id());

    let writer = tokio::spawn(async move {
        write.write_all(b"request").await.unwrap();
        write.shutdown().await.unwrap();
    });
    let reader = tokio::spawn(async move {
        let mut buf = Vec::new();
        read.read_to_end(&mut buf).await.unwrap();
        buf
    });

    let mut request = Vec::new();
    server.read_to_end(&mut request).await.unwrap();
    assert_eq!(request, b"request");
    server.write_all(b"response").await.unwrap();
    server.shutdown().await.unwrap();

    writer.await.unwrap();
    assert_eq!(reader.await.unwrap(), b"response");
}

#[tokio::test]
async fn dropping_write_half_sends_fin_and_keeps_reading() {
    let (_connector, client, mut server) = stream_pair().await;
    let (mut read, mut write) = client.into_split();
    write.write_all(b"last words").await.unwrap();
    drop(write);

    let mut buf = Vec::new();
    server.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, b"last words");

    server.write_all(b"still listening").await.unwrap();
    server.flush().await.unwrap();
    let mut reply = [0u8; 15];
    read.read_exact(&mut reply).await.unwrap();
    assert_eq!(&reply, b"still listening");
}

#[tokio::test]
async fn dropping_read_half_discards_data_and_keeps_writing() {
    let (connector, client, mut server) = stream_pair().await;
    let (read, mut write) = client.into_split();
    drop(read);
    assert_eq!(connector.get_num_tracked_streams(), 1);

    // Far more frames than the client's receive budget of 4. Were they
    // kept, the client would stop reading the carrier.
    tokio::time::timeout(Duration::from_secs(5), async {
        for _ in 0..64 {
            server.write_all(&[0u8; 1024]).await.unwrap();
            server.flush().await.unwrap();
        }
    })
    .await
    .expect("data for a dropped read half stalled the session");

    write.write_all(b"ping").await.unwrap();
    write.flush().await.unwrap();
    let mut buf = [0u8; 4];
    tokio::time::timeout(Duration::from_secs(5), server.read_exact(&mut buf))
        .await
        .expect("write half stopped working")
        .unwrap();
    assert_eq!(&buf, b"ping");

    drop(write);
    // The stream is forgotten only once both halves are gone.
    tokio::time::timeout(Duration::from_secs(5), async {
        while connector.get_num_tracked_streams() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("stream handle outlived both halves");
}
