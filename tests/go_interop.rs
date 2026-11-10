//! Interoperability with the Go reference implementation of smux
//! (github.com/xtaci/smux) under protocol version 2.
//!
//! These tests need a Go toolchain, so they are ignored by default:
//!
//! ```text
//! cargo test --test go_interop -- --ignored
//! ```
//!
//! The peer, `tests/go-interop`, is built in Cargo's temporary directory.
//! `go` downloads github.com/xtaci/smux, unless `SMUX_GO_DIR` names a
//! checkout of it to use instead (through a Go workspace).

use std::{
    num::NonZeroU32,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, OnceLock,
    },
    time::Duration,
};

use async_smux::{MuxBuilder, MuxConnector, MuxStream, ProtocolVersion};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, Join},
    process::{Child, ChildStdin, ChildStdout, Command},
};

type Carrier = Join<ChildStdout, ChildStdin>;

/// The window of both ends. Smaller than the initial 256 KiB, so the first
/// window update shrinks it.
const WINDOW: u32 = 64 * 1024;

fn go_peer() -> &'static Path {
    static PEER: OnceLock<PathBuf> = OnceLock::new();
    PEER.get_or_init(|| {
        let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/go-interop");
        let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("go-interop");
        std::fs::create_dir_all(&dir).unwrap();
        for file in ["go.mod", "main.go"] {
            std::fs::copy(source.join(file), dir.join(file)).unwrap();
        }
        let mut build = std::process::Command::new("go");
        build.current_dir(&dir).env("GOTOOLCHAIN", "local");
        if let Some(smux) = std::env::var_os("SMUX_GO_DIR") {
            let smux = std::fs::canonicalize(smux).unwrap();
            let work = format!("go 1.18\n\nuse (\n\t.\n\t{}\n)\n", smux.display());
            std::fs::write(dir.join("go.work"), work).unwrap();
        } else {
            let _ = std::fs::remove_file(dir.join("go.work"));
            build.env("GOFLAGS", "-mod=mod");
        }
        let binary = dir.join("go-interop");
        let status = build
            .args(["build", "-o"])
            .arg(&binary)
            .arg(".")
            .status()
            .expect("cannot run go");
        assert!(status.success(), "go build failed");
        binary
    })
}

struct Peer {
    connector: MuxConnector<Carrier>,
    _child: Child,
}

fn start_peer() -> Peer {
    let mut child = Command::new(go_peer())
        .arg(WINDOW.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let carrier = tokio::io::join(child.stdout.take().unwrap(), child.stdin.take().unwrap());
    let (connector, _acceptor, worker) = MuxBuilder::client()
        .with_version(ProtocolVersion::V2)
        .with_max_stream_buffer(NonZeroU32::new(WINDOW).unwrap())
        .with_connection(carrier)
        .build();
    tokio::spawn(worker);
    Peer {
        connector,
        _child: child,
    }
}

fn pattern(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(7))
        .collect()
}

async fn open(peer: &Peer, command: u8) -> MuxStream<Carrier> {
    let mut stream = peer.connector.connect().unwrap();
    stream.write_all(&[command]).await.unwrap();
    stream.flush().await.unwrap();
    stream
}

/// Send `data` through an echo stream and check what comes back.
async fn echo(stream: MuxStream<Carrier>, data: Vec<u8>) {
    let (mut reader, mut writer) = tokio::io::split(stream);
    let len = data.len();
    let write = async move {
        writer.write_all(&data).await.unwrap();
        writer.shutdown().await.unwrap();
        data
    };
    let read = async move {
        let mut buf = Vec::with_capacity(len);
        reader.read_to_end(&mut buf).await.unwrap();
        buf
    };
    let (sent, received) = tokio::join!(write, read);
    assert!(sent == received, "echo mismatch");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a Go toolchain"]
async fn bulk_echo() {
    let peer = start_peer();
    tokio::time::timeout(Duration::from_secs(60), async {
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..4 {
            let stream = open(&peer, b'e').await;
            tasks.spawn(echo(stream, pattern(4 << 20)));
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    })
    .await
    .expect("echo deadlocked");
}

#[tokio::test]
#[ignore = "needs a Go toolchain"]
async fn writer_respects_go_window() {
    let peer = start_peer();
    let mut stalled = open(&peer, b's').await;

    let written = Arc::new(AtomicUsize::new(0));
    let writer = tokio::spawn({
        let written = written.clone();
        async move {
            let data = pattern(1 << 20);
            let mut offset = 0;
            while offset < data.len() {
                offset += stalled.write(&data[offset..]).await.unwrap();
                written.store(offset, Ordering::SeqCst);
            }
        }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!writer.is_finished());
    // Go read the command byte and then announced its window; before that
    // we could send the initial 256 KiB.
    let written = written.load(Ordering::SeqCst);
    assert!(
        written <= 256 * 1024,
        "wrote {written} bytes into a shut window"
    );

    // Other streams keep working.
    let stream = open(&peer, b'e').await;
    tokio::time::timeout(Duration::from_secs(10), echo(stream, pattern(1 << 20)))
        .await
        .expect("an unrelated stream stalled");
}

#[tokio::test]
#[ignore = "needs a Go toolchain"]
async fn go_writer_respects_our_window() {
    const LEN: usize = 4 << 20;
    let peer = start_peer();
    let mut flood = open(&peer, b'f').await;
    flood.write_all(&(LEN as u32).to_le_bytes()).await.unwrap();
    flood.flush().await.unwrap();

    // While nobody reads the flood, Go must stay within our window. If it
    // didn't, the session would close and this echo would fail.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let stream = open(&peer, b'e').await;
    tokio::time::timeout(Duration::from_secs(10), echo(stream, pattern(1 << 20)))
        .await
        .expect("an unrelated stream stalled");

    let mut received = Vec::with_capacity(LEN);
    tokio::time::timeout(Duration::from_secs(30), flood.read_to_end(&mut received))
        .await
        .expect("flood did not finish")
        .unwrap();
    assert!(received == pattern(LEN));
}
