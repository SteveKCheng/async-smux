// Command go-interop is the peer for async-smux's interoperability tests:
// a smux server speaking protocol version 2 over its stdin and stdout,
// using the Go reference implementation.
//
// Each stream the client opens starts with a command byte:
//
//	'e'  echo everything back, then half-close
//	's'  never read anything else, so the stream's window stays shut
//	'f'  read a little-endian uint32 n, write n bytes of pattern, half-close
//
// The pattern byte at offset i is byte(i*31 + 7).
package main

import (
	"encoding/binary"
	"io"
	"log"
	"os"
	"strconv"

	"github.com/xtaci/smux"
)

type stdio struct{}

func (stdio) Read(p []byte) (int, error)  { return os.Stdin.Read(p) }
func (stdio) Write(p []byte) (int, error) { return os.Stdout.Write(p) }
func (stdio) Close() error                { return os.Stdout.Close() }

func main() {
	window := 65536
	if len(os.Args) > 1 {
		var err error
		if window, err = strconv.Atoi(os.Args[1]); err != nil {
			log.Fatal(err)
		}
	}
	config := smux.DefaultConfig()
	config.Version = 2
	config.KeepAliveDisabled = true
	config.MaxStreamBuffer = window
	session, err := smux.Server(stdio{}, config)
	if err != nil {
		log.Fatal(err)
	}
	for {
		stream, err := session.AcceptStream()
		if err != nil {
			return // the client closed the carrier
		}
		go serve(stream)
	}
}

func serve(stream *smux.Stream) {
	var command [1]byte
	if _, err := io.ReadFull(stream, command[:]); err != nil {
		log.Print(err)
		return
	}
	switch command[0] {
	case 'e':
		// smux's WriteTo reports the end of the stream as io.EOF.
		if _, err := io.Copy(stream, stream); err != nil && err != io.EOF {
			log.Print(err)
		}
		stream.CloseWrite()
	case 's':
		// Hold the stream without reading it.
	case 'f':
		var n [4]byte
		if _, err := io.ReadFull(stream, n[:]); err != nil {
			log.Print(err)
			return
		}
		data := make([]byte, binary.LittleEndian.Uint32(n[:]))
		for i := range data {
			data[i] = byte(i*31 + 7)
		}
		if _, err := stream.Write(data); err != nil {
			log.Print(err)
		}
		stream.CloseWrite()
	default:
		log.Printf("unknown command %q", command[0])
		stream.Close()
	}
}
