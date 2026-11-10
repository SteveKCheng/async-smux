# async-smux

[![CI](https://github.com/black-binary/async-smux/actions/workflows/ci.yml/badge.svg)](https://github.com/black-binary/async-smux/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/async_smux.svg)](https://crates.io/crates/async_smux)

A lightweight asynchronous [smux](https://github.com/xtaci/smux) (Simple
MUltipleXing) library for Tokio. Wraps any `AsyncRead + AsyncWrite +
Unpin` transport (`TcpStream`, `TlsStream`, …) and lets you spawn many
bi-directional `MuxStream`s over it — each one looks and behaves like a
plain TCP stream.

![img](https://raw.githubusercontent.com/xtaci/smux/master/mux.jpg)

## Quickstart

```rust,ignore
use async_smux::MuxBuilder;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() {
    let tcp = TcpStream::connect("127.0.0.1:12345").await.unwrap();

    // build() returns three pieces:
    //   connector — open new outgoing streams
    //   acceptor  — receive peer-initiated streams
    //   worker    — the future that drives I/O; spawn it
    let (connector, mut acceptor, worker) =
        MuxBuilder::client().with_connection(tcp).build();
    tokio::spawn(worker);

    // Outgoing
    let mut s = connector.connect().unwrap();
    s.write_all(b"hello").await.unwrap();
    let mut buf = [0u8; 5];
    s.read_exact(&mut buf).await.unwrap();

    // Incoming
    while let Some(mut peer) = acceptor.accept().await {
        // ...
    }
}
```

The server side is identical except for `MuxBuilder::server()`. Client
and server differ only in stream-id parity (odd vs. even) so two peers
never collide on locally-allocated ids.

A complete working example is in [`examples/echo.rs`](examples/echo.rs).

## Lifecycle

Three handles share ownership of the connection state:

- **`MuxConnector`** — opens streams. `Clone`-able.
- **`MuxAcceptor`** — receives peer-initiated streams. Implements
  `Stream<Item = MuxStream>`.
- **`MuxWorker`** — the future that drains the underlying transport
  and dispatches frames. Spawn it with `tokio::spawn(worker)`.

The worker exits when:

- All public handles (connectors + acceptors + streams) are dropped, or
- `MuxConnector::close().await` is called explicitly, or
- The peer closes the transport, or
- A keep-alive timeout fires (if configured).

`close()` performs an orderly shutdown: any frames already accepted by
`AsyncWrite::poll_write` are drained to the wire before the underlying
sink is closed. It also works without the worker being polled — useful
in test setups or sync teardown paths.

`MuxStream` supports TCP-style half-close. Calling
`AsyncWriteExt::shutdown()` sends FIN and closes only the local write
direction; reads remain open until the peer sends its FIN. Likewise, a
peer FIN produces read EOF without preventing a response from being
written. Dropping a stream drains writes already accepted by
`poll_write`, sends FIN if needed, and releases the local stream handle.
The wire format is unchanged; a peer must also interpret FIN as directional
EOF to send data after receiving it.

## Configuration

```rust,ignore
use async_smux::{MuxBuilder, ProtocolVersion};
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};

let (connector, acceptor, worker) = MuxBuilder::server()
    // Protocol version 2 adds per-stream flow control. Both ends must
    // use the same version; smux does not negotiate it.
    .with_version(ProtocolVersion::V2)
    // Receive window per stream (v2 only). Defaults to 256 KiB.
    .with_max_stream_buffer(NonZeroU32::new(256 * 1024).unwrap())
    // Send a NOP frame every N seconds to keep the connection alive.
    .with_keep_alive_interval(NonZeroU64::new(15).unwrap())
    // If we don't see any frame from the peer for this many seconds,
    // declare the connection dead and tear everything down.
    // Defaults to 3 × keep_alive_interval.
    .with_keep_alive_timeout(NonZeroU64::new(45).unwrap())
    // Per-stream idle timeout (seconds): close streams with no
    // recent traffic.
    .with_idle_timeout(NonZeroU64::new(60).unwrap())
    // Backpressure thresholds: cap queued tx frames and the combined
    // inbound-frame/unaccepted-stream backlog (with v2, only unaccepted
    // streams). Keep-alive expiry pauses while the RX budget
    // deliberately prevents carrier reads.
    .with_max_tx_queue(NonZeroUsize::new(1024).unwrap())
    .with_max_rx_queue(NonZeroUsize::new(1024).unwrap())
    .with_connection(connection)
    .build();
```

All of these knobs are optional. Keep-alive and idle timeout are off
unless explicitly enabled.

## Protocol versions

With version 1 (the default), streams have no flow control of their
own. Inbound data waits in a session-wide budget (`max_rx_queue`), and
when that is used up, the carrier stops being read: a single stream
whose reader has stopped stalls every other stream.

Version 2, as in the Go implementation (`Config.Version = 2`), gives
each stream a window. The receiver reports with UPD frames how much it
has consumed, and a writer waits once the peer's window is full, so a
stalled stream only stalls its own writer. A peer that sends more than
the window is a protocol error. See `docs/flow-control.md`.

## Frame format

```text
VERSION(1B) | CMD(1B) | LENGTH(2B LE) | STREAMID(4B LE) | DATA(LENGTH)

VERSION: 1 or 2, as configured; any other value is a protocol error
CMD:
    SYN(0)  open stream  (LENGTH must be 0)
    FIN(1)  close sender's write direction (LENGTH must be 0)
    PSH(2)  payload
    NOP(3)  keep-alive   (LENGTH must be 0; STREAMID is 0)
    UPD(4)  window update, v2 only (LENGTH must be 8):
            CONSUMED(4B LE) | WINDOW(4B LE)
            CONSUMED counts the bytes read from the stream so far,
            modulo 2^32; WINDOW is the receiver's window
```

Stream id 0 is reserved for NOP; the library never hands it out and
rejects any peer SYN that uses it.

## Benchmark

| Implementation    | Throughput (TCP) | Handshake  |
| ----------------- | ---------------- | ---------- |
| smux (go)         | 0.4854 GiB/s     | 17.070 K/s |
| async-smux (rust) | 1.0550 GiB/s     | 81.774 K/s |

`benches/bench.rs` uses the unstable `test` crate so it requires a
nightly toolchain: `cargo +nightly bench`.

## License

MIT — see [LICENSE](LICENSE).
