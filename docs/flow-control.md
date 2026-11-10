# Flow control, and other gaps in async-smux

Notes from using async-smux 0.4.0 (commit 0019a7f) as the multiplexer of
[Shion](https://github.com/SteveKCheng/shion), where many SSH channels share
one carrier: interactive shells, command output, sftp, port forwards. None
of the problems below are specific to Shion; any user that mixes streams
whose readers can stop is exposed to the first one.

Status on the `flow-control-v2` branch:

| Issue                                 | Status                         |
|---------------------------------------|--------------------------------|
| No per-stream flow control            | implemented as protocol v2     |
| `poll_flush` fails after shutdown     | not fixed yet (§2)             |
| No way to abort a stream (RST)        | not started (§3)               |

## 1. Flow control

### 1.1 The problem

async-smux 0.4.0 implements smux **protocol version 1**: SYN, FIN, PSH and
NOP. Streams have no window. Inbound data waits in a receive budget,
`max_rx_queue`, which counts *frames* across the whole session (plus
unaccepted streams). Once the budget is used up, the dispatcher stops
reading the carrier altogether.

So **one stream whose reader stops stalls every stream on the carrier.**
In Shion, if an SSH client stops reading a port forward, the interactive
shell on the same carrier freezes too. Over a local pipe the stall needs a
reader that really stops, so it rarely happens, but nothing prevents it,
and over a network it would bite much sooner.

### 1.2 Protocol version 2 in the Go implementation

The original implementation, [xtaci/smux](https://github.com/xtaci/smux),
added flow control as protocol version 2 (checked at v1.5.57-1, ae956bb).
It adds one frame type:

```text
UPD(4)  LENGTH = 8, payload: CONSUMED(4B LE) | WINDOW(4B LE)
```

- **Receiver:** `CONSUMED` is the total number of bytes read from the
  stream so far, modulo 2^32. It sends a UPD on the first read from a
  stream, and then whenever another half of its `MaxStreamBuffer` has been
  read since the last one. `WINDOW` is always `MaxStreamBuffer` (64 KiB by
  default). UPDs go out on the control path, ahead of data.
- **Sender:** may send `peerWindow - (numWritten - peerConsumed)` more
  bytes, computed with wrapping 32-bit arithmetic. Until the first UPD
  arrives it assumes a window of 256 KiB (`initialPeerWindow`). With no
  room left, `Write` blocks until a UPD arrives.
- **Version byte:** every frame carries it, and there is no negotiation.
  A session configured for version 2 treats a version-1 frame as a
  protocol error, and the other way round. Both ends must be configured
  alike.

What Go's version 2 does *not* do:

- The receiver never checks that the peer stays within the window.
- The session-wide token bucket (`MaxReceiveBuffer`, 4 MiB) stays, and its
  receive loop still stops reading when the bucket runs dry. So Go bounds
  head-of-line blocking rather than removing it: about 16 stalled streams
  × 256 KiB fill the bucket and stall the session again.
- `Write` does not wake up when the peer sends FIN, only on a UPD, a local
  close, a deadline or a session error (see §1.6).

Two quirks found while testing against it:

- `stream.WriteTo` (used by `io.Copy`) returns `io.EOF` at the end of the
  stream instead of `nil`.
- `MaxStreamBuffer` must fit in an `int32`, since the window arithmetic is
  signed.

### 1.3 Design here

The aim was to follow the Go wire protocol exactly, so the two
interoperate, but to go further on the receiving side so that a stalled
stream really cannot block the session.

**Configuration.** `MuxBuilder::with_version(ProtocolVersion::V2)` and
`with_max_stream_buffer(n)`; `MuxConfig` gains the fields `version` and
`max_stream_buffer`. `ProtocolVersion` is a `#[non_exhaustive]` enum so
that later variants (an RST extension, §3) can be added without another
breaking change. V1 stays the default and behaves exactly as before.

The default window is 256 KiB rather than Go's 64 KiB: it equals the
window a peer assumes initially, so the first UPD doesn't shrink it, and a
64 KiB window would cut throughput over a fast carrier. Windows above
`i32::MAX` are clamped, for Go's sake.

**Codec** (`frame.rs`). `MuxCodec` carries the session's version, stamps
it on every frame it encodes and rejects any other version when decoding.
`MuxFrameHeader` no longer stores a version. A UPD must have length 8 and
a non-zero stream id, and is an invalid command under V1.

**Sending** (`mux.rs`). Each `StreamHandle` keeps `num_written`,
`peer_consumed` and `peer_window`. `poll_stream_write_ready` returns how
many bytes the stream may take, and `poll_write` caps the frame at that.
With no room left it parks on the stream's `tx_done_waker`, and the
dispatcher wakes it when a UPD arrives. The window is checked only before
a frame is queued, so frames already queued, FIN, `poll_flush` and
`poll_shutdown` never wait on it. A UPD claiming that more was consumed
than was sent (a negative 32-bit difference) closes the session with
`InvalidWindowUpdate`. UPDs for unknown streams are ignored, as in Go.

**Receiving.** Each handle tracks `rx_buffered` (bytes in its `rx_queue`)
and `num_read`. Data counts as consumed when it leaves `rx_queue`, which is
at most one frame (64 KiB) ahead of what the reader has actually taken;
counting there avoids locking the read buffer that `MuxStream` holds
outside the shared state. UPDs follow Go's rule (first read, then every
half window) and go on the control queue. None is sent after the peer's
FIN, since no more data is coming.

**The session-wide budget.** With V2, stream data no longer counts against
`max_rx_queue`; only unaccepted streams do. Each stream's data is bounded
by its window instead: a peer may have in flight at most the window it
last heard of, which is ours or, before our first UPD, the initial
256 KiB. So a stream can never legitimately buffer more than
`max(256 KiB, max_stream_buffer)`, and a peer that sends more closes the
session with `WindowExceeded`. Memory stays bounded (per stream by the
window, and unaccepted streams by `max_rx_queue`), and the carrier is
never left unread because of one stream's backlog.

**Errors.** New `MuxError` variants: `InvalidUpdatePayload`,
`InvalidWindowUpdate`, `WindowExceeded`.

### 1.4 Compatibility

- V1 sessions behave as before; the existing tests pass unchanged except
  for constructing `MuxCodec` with a version.
- The public API change is additive in spirit but breaking for semver:
  `MuxConfig` has new public fields (code that builds it with a struct
  literal must set them) and `MuxError` has new variants (exhaustive
  matches break). The next release should be 0.5.
- Mixing versions fails at the first frame, by design of the protocol.

### 1.5 Tests

- `src/flow_control_tests.rs`: a stalled stream doesn't block another and
  its writer stops at exactly the initial window; bulk transfers with
  small (8 KiB) windows in both directions; UPDs on first read and after
  half a window; a writer that waits for and is woken by UPDs, and never
  exceeds the window; a peer overrunning the window, or acknowledging
  unsent data, closes the session; UPDs for unknown streams are ignored;
  codec round trip and rejection of malformed UPDs and wrong versions.
- `src/mux.rs` (`alloc_tests`): window arithmetic across the 32-bit wrap.
- `tests/go_interop.rs`, against the Go implementation: bulk echo over
  four streams, our writer respecting Go's window, and Go's writer
  respecting ours while another stream keeps working. These need a Go
  toolchain and are ignored by default:

  ```text
  cargo test --test go_interop -- --ignored
  SMUX_GO_DIR=~/dev/smux-go cargo test --test go_interop -- --ignored
  ```

  The peer (`tests/go-interop`) is built in Cargo's temporary directory.
  Without `SMUX_GO_DIR`, `go` downloads `github.com/xtaci/smux`; with it,
  a Go workspace uses that checkout instead, which works offline.

I checked by breaking the code on purpose that the window-overrun test
fails without the overrun check, and the Go test fails when the window is
ignored.

### 1.6 Known gaps

- **A writer to a stream the peer has abandoned can block for good.**
  When the peer drops a stream, it sends FIN, but FIN only means "I won't
  write any more"; the peer might still be reading after a half-close. So
  FIN can't release a writer waiting for window, and with no UPD coming,
  it waits until the stream is dropped locally or the session ends. (Go
  behaves the same, short of a write deadline.) Under V1 such writes went
  into the void instead, which wasn't better. The real fix is a reset
  frame (§3).
- Unaccepted streams still use the session-wide budget, so an application
  that stops accepting still stalls the session once `max_rx_queue` SYNs
  are waiting. Refusing excess SYNs with FIN instead would fix that.
- Consumption is counted one frame early (see above), so the memory bound
  per stream is the window plus one frame.

### 1.7 Effort

Before starting, I estimated 250–400 lines in the crate and about as much
again in tests, with the receive-side budget as the only part needing
real design. The result: about 310 lines added to the library itself,
670 lines of Rust tests, and a 90-line Go peer for the interop tests.

## 2. `poll_flush` fails once shutdown has started

Once `MuxStream::poll_shutdown` has queued the FIN (which marks the write
half closed), `poll_flush` returns `BrokenPipe` ("stream tx is already
closed") as soon as the queue has drained, instead of `Ok`.

That breaks `tokio_util::codec::Framed::close` (and `SinkExt::close`):
`Framed::poll_close` flushes and then calls `poll_shutdown`, and when
shutdown returns `Pending` while the FIN is still being written, the whole
sequence is polled again, and the second flush fails. Reproduced at
0019a7f: `poll_shutdown` returns `Pending`, the FIN goes out, and the next
`poll_flush` returns the error.

Flushing a half-closed stream should succeed once everything queued,
FIN included, is on the carrier; only `poll_write` should fail. Shion
works around it with a wrapper (`shion_mux::Stream`) that remembers that
shutdown has started and turns a later flush into another
`poll_shutdown`. The fix in `MuxStream::poll_flush` is small: return `Ok`
rather than `BrokenPipe` when the stream's own write half was closed by
`poll_shutdown` (as opposed to by a session close). Not done on this
branch yet.

## 3. No way to abort a stream

smux has only FIN: half-close. Even dropping a `MuxStream` sends FIN. QUIC
and TCP distinguish an orderly end of data from aborting the stream (QUIC's
RESET_STREAM and STOP_SENDING, TCP's RST). Without that:

- A writer can't tell that the peer has abandoned the stream (§1.6).
- An application can't pass on "this channel is gone in both directions".
  In Shion, an SSH client's `CHANNEL_CLOSE` can only become a FIN, which a
  process on a terminal never notices.

The likely fix is a RST frame, as in TCP: "I am discarding this stream;
stop sending, and I won't send any more." Go's smux has no such frame, so
it would be an extension beyond version 2, with a command number Go
doesn't use, and enabled only when both ends are configured for it. That
is what the `ProtocolVersion` enum leaves room for. The frame layer work is
about the same size as UPD's; the semantics (what a reset does to queued
data on each side, and to a reader) need deciding first.
