# Aborting streams: RESET and STOP_SENDING

**Status: draft proposal, not implemented.** This builds on protocol
version 2 (`docs/flow-control.md`). It needs both ends to be configured
for it, and the Go implementation doesn't support it.

## 1. The problem

A smux stream is two independent one-way byte channels, one in each
direction. Since async-smux 0.4 each direction can end on its own (FIN is a
half-close). But FIN is the only way to end one, and it means just one
thing: the *writer* has *finished*. Two other endings can't be expressed:

| A direction ends because…            | Sent by | Local analogue                         | smux today | QUIC          |
|--------------------------------------|---------|----------------------------------------|------------|---------------|
| the writer has finished              | writer  | `close` or `shutdown(SHUT_WR)`: EOF    | FIN        | FIN bit       |
| the writer abandons it (truncated)   | writer  | none, short of TCP's RST on close      | —          | RESET_STREAM  |
| the reader abandons it               | reader  | closing a pipe's read end: `EPIPE`     | —          | STOP_SENDING  |

The last two come from *opposite* ends of the same direction. TCP's RST
mixes them up and also takes the other direction with it. QUIC separates
them, and that's the model proposed here.

**Not TCP's RST.** In TCP, RST aborts the whole connection, both
directions at once. The RST proposed here is QUIC's RESET_STREAM under a
shorter name: it ends only the direction its sender writes. The other
direction carries on, and ends on its own with FIN or RST from the other
side. Aborting a stream entirely takes RST and STOP together (§3.4).

Consider a process's stdin and stdout plumbed over a stream. Locally, the
process can close its stdin while still writing to stdout, and a writer
feeding that stdin gets `EPIPE` (or `SIGPIPE`) on its next write. The same
works for a full-duplex local socket. On a Unix socket pair,
`shutdown(SHUT_RD)` on one end makes the peer's writes fail with `EPIPE`,
while the other direction keeps working. Over TCP, `SHUT_RD` is purely
local: nothing goes on the wire, and the peer carries on writing into the
void. smux is in TCP's position.

What goes wrong without the two frames:

- **A writer can't learn that its reader has gone.** With protocol
  version 2 this is worse than an annoyance. A writer whose peer dropped
  the stream waits forever for a window update that will never come
  (`docs/flow-control.md` §1.6). Under version 1, the data went into the
  void instead.
- **A reader can't tell a truncated stream from a complete one.** A writer
  that gives up (its process was killed, say) can only send FIN, which
  looks like a normal end of data.
- **An application can't abort a stream.** For example, an SSH server that
  receives `CHANNEL_CLOSE` before its command has finished can pass on
  only a FIN. A process on a terminal never notices that.

Some of the distinctions below may look pedantic when the application on
top, such as SSH, can't express them. But the multiplexer has to give
*some* meaning to a direction that is abandoned rather than finished, and
it is easier to reason about, and more robust, if the two directions stay
independent, each ending in its own way, than with TCP's RST, which ends
both at once.

## 2. Protocol: version byte 255

The extension gets its own version byte, **255**, under the name
`ProtocolVersion::V2WithReset`. It includes everything in version 2 (UPD
and per-stream flow control). Since smux doesn't negotiate versions, both
ends must be configured for it. A version-2 or Go peer rejects the first
frame, and the session closes at once rather than misbehaving later.
Using 255 rather than 3 keeps clear of whatever versions the Go
implementation may define next.

Two new commands, valid only under version 255:

```text
RST(5)   LENGTH = 8: ERROR_CODE(8B LE)
         Sent by a writer: "I abandon my direction of this stream."
         Unlike TCP's RST, the other direction is unaffected.
STOP(6)  LENGTH = 8: ERROR_CODE(8B LE)
         Sent by a reader: "Stop sending; I won't read any more."
```

Both need a non-zero stream id. Any other length is a protocol error, as
for UPD.

**Error codes** are 64-bit and opaque to the multiplexer. 0 means "no
error given", and it's the code async-smux uses when it sends these frames
on its own (§4). Applications define the rest. 64 bits hold QUIC's 62-bit
application error codes without loss, or a sparse encoding of an
application's own. The frames are rare, so the extra bytes cost nothing,
and a wider field now saves another incompatible protocol change later.

**No final size.** QUIC's RESET_STREAM carries the final size of the
stream, so that the receiver can settle connection-level flow-control
credit. Our windows are per stream only, and a stream's credit disappears
with it, so the field isn't needed.

## 3. Semantics

Each direction of a stream ends with exactly one *terminal frame* from its
writer: FIN or RST. STOP doesn't end the direction itself; it asks the
writer to end it.

### 3.1 RST: the writer abandons its direction

The sender:

- discards the stream's PSH frames that are still queued (data already
  written to the carrier can't be recalled);
- queues the RST on the control queue, ahead of other streams' data, since
  nothing of this stream remains to wait for;
- accepts no more writes on the stream. `poll_write` fails with
  `BrokenPipe`, as after a shutdown.

An RST is allowed only while the write half is open. Once FIN has been
queued, the direction has already ended, and `reset` returns an error.

The receiver:

- discards any data of that direction it has buffered but not yet read,
  and frees it from the stream's window accounting;
- fails the next `poll_read` with `io::ErrorKind::ConnectionReset`, the
  inner error being `MuxError::StreamReset(code)`. Data that the reader had
  already taken out of the queue is unaffected;
- sends no more UPDs for that direction, and ignores later PSH frames for
  it.

Discarding buffered data at once, rather than delivering it first, follows
TCP's RST. It frees memory straight away, and the writer chose RST over
FIN precisely because the data isn't a complete whole. (QUIC leaves this
choice to implementations.)

### 3.2 STOP: the reader abandons the other direction

The sender (the reader of that direction):

- discards its buffered unread data for that direction, and every PSH
  that arrives for it from then on;
- sends no more UPDs for it;
- makes later `poll_read` calls return end-of-file. This is what
  `shutdown(SHUT_RD)` does to a local socket, and the reader asked for it.
  Its own write direction isn't affected.

The receiver (the writer of that direction) must reply with **RST,
echoing the code**, unless it has already queued FIN. As with QUIC, this
gives the direction a definite terminal frame, so the reader knows when no
more data for it will come (§3.3). The writer sends the RST at once: it
concerns only the writer's own direction, which it can no longer use
anyway. Apart from sending the RST (§3.1), it:

- fails the next `poll_write`, and wakes any writer waiting for window,
  with `io::ErrorKind::BrokenPipe`, the inner error being
  `MuxError::StopSending(code)`. That is the `EPIPE` of a local pipe,
  without the signal;
- keeps its own read direction open: the peer may still be sending.

If FIN has already been queued, no reply is needed: the FIN is the
terminal frame. Queued PSH frames ahead of it may still go out, and the
peer discards them.

### 3.3 When a direction, and a stream, has ended

Ending a direction and ending the stream are different things. A
direction has ended for its writer once the writer has sent its terminal
frame, and for its reader once that frame has arrived. The stream as a
whole has ended for a peer once it has *sent* its own terminal frame and
*received* the other side's.

So STOP is the reading counterpart of `shutdown(SHUT_RD)`, and the peer
that sends it can go on writing. Nothing waits for the other side's
direction to end:

```text
A                                    B
|-- STOP(code) ------------------->  |  B's writes now fail (EPIPE-like)
|  <------------------ RST(code) --  |  B→A has ended: sent at once
|-- PSH ... PSH ------------------>  |  A→B carries on as normal;
|  <------------------------ UPD --  |  B keeps reading and granting window
|-- FIN -------------------------->  |  A→B has ended: the stream is over
```

**The terminal frame isn't the last frame about a stream.** UPD and STOP
travel against the data, from a direction's reader to its writer. In the
diagram, B goes on sending UPDs for A→B after its own RST, until A's FIN
arrives. So when a peer has finished with a stream, the other side may
still have a UPD or STOP for it in flight, and no frame announces the
last one. (TCP has the same problem with FIN, and solves it with
TIME_WAIT. QUIC never reuses stream ids.)

Frames for streams that no longer exist are ignored, so a late frame is
harmless unless its stream id has been reused in the meantime. Then it
would be taken for a frame of the new stream: a stale UPD would grant
window to the wrong one. In async-smux, a stream id is reused only once
one side's 2^31 ids have wrapped around, and only if the old stream's
handle is gone. We keep that behaviour: a long-lived session must be
able to wrap around, and on a local carrier, where smux belongs (remote
multiplexing is better served by QUIC), no frame stays in flight for
anything like the time a wrap-around takes.

A stream's handle should still be kept until it has ended in both
directions as defined above, so that frames racing the end of the stream
are recognised, not answered as if the stream were unknown. Today, a PSH
for an unknown stream gets a FIN in reply.

### 3.4 Odds and ends

- **Both directions at once:** send RST and STOP. That is how to pass on
  SSH's `CHANNEL_CLOSE`.
- **Frames for unknown streams:** an RST or STOP for a stream that no
  longer exists is ignored, as UPD is.
- **Session close** (`MuxConnector::close`, carrier errors) sends no
  per-stream frames. Ending the carrier ends every stream, as now.

## 4. What async-smux sends on its own

Under version 255 only; versions 1 and 2 behave as now.

| Event                                                     | Today       | With 255                       |
|-----------------------------------------------------------|-------------|--------------------------------|
| `MuxStream` dropped, write half open                      | FIN         | FIN; RST(0) with reset-on-drop |
| `MuxStream` dropped, peer's direction not yet ended       | FIN in reply to later data | STOP(0)         |
| read half dropped (§5.2), peer's direction not yet ended  | —           | STOP(0)                        |
| write half dropped (§5.2), write half open                | —           | FIN; RST(0) with reset-on-drop |
| acceptor dropped with streams not yet accepted            | FIN         | RST(0) and STOP(0)             |
| idle timeout                                              | FIN         | RST(0) and STOP(0)             |
| STOP received, write half open                            | —           | RST(code from STOP)            |

By default, dropping a writer still sends FIN, not RST. `Drop` can't
tell success from failure, data accepted by `poll_write` is delivered
today, and callers rely on that (as with Tokio's `OwnedWriteHalf`).
Applications that finish their streams explicitly with `shutdown()` can
opt in to *reset-on-drop* (§5.3), which makes a drop with the write half
still open mean "abort". Dropping a reader that still has data coming,
on the other hand, is exactly the case for STOP: like closing a socket
with unread data, which makes TCP send RST. It needs no option.

For refused and timed-out streams, RST plus STOP is more honest than FIN:
the peer's writes fail instead of disappearing.

## 5. API

### 5.1 Methods on `MuxStream`

```rust
impl<T: TokioConn> MuxStream<T> {
    /// Abandon our write direction: discard queued data and send RST.
    /// Fails if the write half is already closed, or if the session's
    /// protocol version doesn't support resets.
    pub fn reset(&mut self, code: u64) -> MuxResult<()>;

    /// Abandon the peer's direction: discard unread data and send STOP.
    /// Later reads return end-of-file.
    pub fn stop_reading(&mut self, code: u64) -> MuxResult<()>;

    /// Both of the above, skipping a direction that has already ended.
    pub fn abort(&mut self, code: u64) -> MuxResult<()>;
}
```

They are synchronous, like quinn's `SendStream::reset` and
`RecvStream::stop`: they only queue a frame. Under versions 1 and 2 they
return a new `MuxError::Unsupported`, rather than falling back to FIN with
different semantics.

`AsyncRead` and `AsyncWrite` have no way to report the codes, so they
arrive as the inner error of an `io::Error`:

| Error seen                  | `io::ErrorKind`   | inner error                   |
|-----------------------------|-------------------|-------------------------------|
| `poll_read` after peer RST  | `ConnectionReset` | `MuxError::StreamReset(code)` |
| `poll_write` after peer STOP| `BrokenPipe`      | `MuxError::StopSending(code)` |

Today `mux_to_io_err` maps every `MuxError` to `ErrorKind::Other`, so
these two need their own kinds. A small helper, say
`MuxError::from_io(&io::Error) -> Option<&MuxError>`, would save callers
from doing the downcast themselves.

### 5.2 Owned read and write halves

Dropping a reader should send STOP(0), but Tokio's generic
`tokio::io::split` can't do that. Its `ReadHalf` only knows the
`AsyncRead` trait, and dropping it does nothing; STOP goes out only once
both halves are gone and the `MuxStream` itself is dropped.

So async-smux should offer its own split, like Tokio's
`TcpStream::into_split`:

```rust
impl<T: TokioConn> MuxStream<T> {
    pub fn into_split(self) -> (MuxReadHalf<T>, MuxWriteHalf<T>);
}
```

- `MuxReadHalf<T>`: `AsyncRead`, `stop_reading`, `get_stream_id`.
  Dropping it sends STOP(0) if the peer's direction hasn't ended.
- `MuxWriteHalf<T>`: `AsyncWrite`, `reset`, `get_stream_id`. Dropping it
  while the write half is open sends FIN, as dropping a `MuxStream` does
  now, or RST(0) with reset-on-drop (§5.3).
- Optionally `reunite`, as Tokio has.

This is cheaper than Tokio's generic split, which needs a lock between
the halves. A `MuxStream` is only a stream id, a handle on the shared
session state (already behind its mutex), and a read buffer that belongs
to the reader. The halves just need to share the stream's handle, which
is removed once both are dropped.

`into_split` is useful under versions 1 and 2 too: there, dropping the
read half simply does nothing.

### 5.3 Reset on drop

```rust
impl MuxBuilder<WithConfig> {
    /// With ProtocolVersion::V2WithReset: dropping a stream, or its write
    /// half, while the write half is still open sends RST(0) instead of
    /// FIN, discarding data not yet written to the carrier. Finish streams
    /// with `AsyncWriteExt::shutdown` to end them gracefully. Off by
    /// default; ignored with other versions.
    pub fn with_reset_on_drop(&mut self, on: bool) -> &mut Self;
}
```

It is off by default even under version 255, because it changes what
dropping a stream means, and existing callers count on a drop delivering
what they wrote. Precedents differ too: TCP's `close` delivers unsent data
and sends FIN, while HTTP/2 libraries reset a stream that is dropped
before it was finished. An option set once, where the session is built,
lets each application choose.

## 6. Using it through `AsyncRead` and `AsyncWrite` alone

An application may want to stay independent of the multiplexer, so that
its stream code works over any carrier, and to call nothing
smux-specific except where the session is built. With reset-on-drop, the
whole extension is reachable that way, through one rule:

- **Finish with `shutdown()`.** `AsyncWriteExt::shutdown`, or `close` on a
  `Framed` sink, which calls it, sends FIN after everything written.
- **Abort by dropping.** Dropping a stream whose write half is still open
  sends RST(0); dropping one whose peer is still sending sends STOP(0).
- **Read errors mean abort.** The peer's RST arrives as
  `ErrorKind::ConnectionReset` from `poll_read`, distinct from a clean
  end-of-file, and its STOP as `ErrorKind::BrokenPipe` from `poll_write`.
  Code that treats any read error as "the other side has gone" handles
  both without knowing about smux.

With a carrier that can't abort, the same code degrades gracefully: the
rule is still correct, and only carries less information.

| Carrier                                  | drop, write half open | peer's abort seen as        |
|------------------------------------------|-----------------------|-----------------------------|
| smux version 255 with reset-on-drop      | RST(0) (+ STOP(0))    | `ConnectionReset` / `BrokenPipe` |
| smux version 255 without it              | FIN (+ STOP(0))       | end-of-file / `BrokenPipe`  |
| smux versions 1 and 2, pipes, TCP        | FIN                   | end-of-file                 |

What can't be reached through the traits alone is `stop_reading` while
still writing, and error codes other than 0. Splitting a stream with the
generic `tokio::io::split` or `futures`' `StreamExt::split` (as on a
`Framed`) also means nothing is sent until *both* halves are dropped. An
application that splits a stream across tasks must make an abort end
both tasks, not just the one that noticed it.

**Example: an SSH server.** Shion's front-end turns SSH channels into smux
streams to a session process:

- `CHANNEL_EOF` from the client becomes `close()` on the stream's sink: a
  FIN, as today.
- `CHANNEL_CLOSE` before the session has finished, or the client
  disconnecting, becomes dropping the stream, which with reset-on-drop
  sends RST(0) and STOP(0). Today it closes the sink, which sends FIN.
- The session process already treats a read error on a channel's stream
  as a hangup. So the RST hangs up the channel's processes, even on a
  terminal, where a FIN would only have closed stdin, which a terminal
  never sees. No session code changes; only the front-end's carrier setup
  (`with_version`, `with_reset_on_drop`) and its choice between closing
  and dropping.

## 7. What this fixes

- **The blocked writer of `docs/flow-control.md` §1.6:** a dropped reader
  sends STOP, and the waiting writer fails with `BrokenPipe` instead of
  waiting forever.
- **Truncation is visible:** a reader sees `ConnectionReset` rather than a
  clean end-of-file.
- **Application-level aborts:** an SSH front-end can turn `CHANNEL_CLOSE`
  into `abort`, or with reset-on-drop simply drop the stream (§6), so the
  far end's process gets a hangup even on a terminal.
  If a process closes its stdin early, the session can `stop_reading` that
  direction, and the other side's writer learns of it, which SSH itself
  can't express.

What it doesn't fix:

- **SSH2 and SSH3 can't use per-direction aborts end to end.** SSH2 has
  only `CHANNEL_EOF` and `CHANNEL_CLOSE` (RFC 4254 §5.3), and nothing to
  say "stop sending to me" while keeping one's own direction open.
- **stderr isn't separate.** SSH2 and SSH3 both carry stderr in the same
  channel as stdout (`CHANNEL_EXTENDED_DATA`), and an EOF covers both. A
  protocol that wants stderr to end or be aborted on its own has to give
  it a stream of its own. smux streams are bidirectional, so that would be
  a stream FIN'd in one direction straight away. With FIN, RST and STOP
  per direction, the transport would then have everything needed.

## 8. Implementation notes

- **Frame layer:** `MuxCommand::Reset` and `MuxCommand::StopSending`,
  accepted only under version 255, with an 8-byte payload. This is about
  the same work as UPD was.
- **State per direction:** `StreamHandle` already tracks `read_closed` and
  `write_closed`. It gains the cause of each, which the error codes and
  the "has FIN been queued?" checks need: finished, reset with a code, or
  stopped with a code.
- **Queues:** RST means purging the stream's `tx_queue` before queueing on
  the control queue. Waking writers parked on window or on a full queue
  is done with the existing `notify_tx_done`.
- **Drop paths:** `MuxStream::drop`, `close_unaccepted_streams` and the
  idle-timeout reaper change as in §4, for version 255 only. With
  reset-on-drop, `MuxStream::drop` purges the stream's `tx_queue` and
  queues RST instead of moving the queue to the global one with a FIN.
- **Tests:** each row of §4, with and without reset-on-drop; RST and STOP
  while data is queued, buffered, or in flight; RST racing FIN; STOP while a writer waits for window; the
  error kinds and codes; `into_split` drops. There can be no Go interop
  test, since Go has no such frames, beyond checking that a Go peer
  rejects version 255 at once.

The size is probably similar to protocol version 2's: a few hundred lines
in the library, more in tests.
