// Command sink-test-frames listens on a UDP port and counts incoming BSV
// transaction frames delivered by shard-listener. When -count frames
// have been received it exits 0. If -timeout expires first it exits 1.
//
// Use -raw to count raw UDP datagrams without frame decoding; pass this flag
// when testing strip-header mode where the listener strips the 92-byte BRC-124/BRC-128
// frame header before forwarding.
//
// Use -tcp -bytes N to stand in for a bridge on a reference lane (-lane-*):
// accept TCP connections and exit 0 once N bytes in total have arrived.
//
// Usage:
//
//	sink-test-frames [-port N] [-count N] [-raw] [-timeout D]
//	sink-test-frames -tcp -bytes N [-port N] [-timeout D]
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lightwebinc/shard-common/frame"
)

func main() {
	port := flag.Int("port", 9102, "UDP port to listen on")
	count := flag.Int("count", 0, "exit 0 after receiving this many items (0 = run forever)")
	raw := flag.Bool("raw", false, "count raw datagrams without frame decode (for strip-header testing)")
	timeout := flag.Duration("timeout", 30*time.Second, "exit 1 if -count not reached within this duration (0 = no timeout)")
	useTCP := flag.Bool("tcp", false, "accept TCP connections and count bytes (reference lane stand-in); needs -bytes")
	wantBytes := flag.Int64("bytes", 0, "with -tcp: exit 0 after receiving this many bytes in total")
	flag.Parse()

	if *useTCP {
		os.Exit(tcpSink(*port, *wantBytes, *timeout))
	}

	conn, err := net.ListenPacket("udp", fmt.Sprintf("127.0.0.1:%d", *port))
	if err != nil {
		log.Fatalf("listen udp 127.0.0.1:%d: %v", *port, err)
	}
	defer func() { _ = conn.Close() }()

	log.Printf("sink: listening on 127.0.0.1:%d  count=%d  raw=%v  timeout=%s",
		*port, *count, *raw, *timeout)

	var received atomic.Int64
	done := make(chan struct{})

	go recvLoop(conn, *count, *raw, &received, done)

	if *timeout > 0 {
		timer := time.NewTimer(*timeout)
		defer timer.Stop()
		select {
		case <-done:
			os.Exit(0)
		case <-timer.C:
			log.Printf("sink: TIMEOUT after %s: only received %d/%d", *timeout, received.Load(), *count)
			os.Exit(1)
		}
	} else {
		<-done
		os.Exit(0)
	}
}

func recvLoop(conn net.PacketConn, limit int, raw bool, received *atomic.Int64, done chan struct{}) {
	buf := make([]byte, 65536)
	for {
		n, src, err := conn.ReadFrom(buf)
		if err != nil {
			return
		}
		if raw {
			cnt := received.Add(1)
			fmt.Printf("recv  src=%-26s  bytes=%d\n", src, n)
			if limit > 0 && int(cnt) >= limit {
				log.Printf("sink: received %d datagrams", cnt)
				close(done)
				return
			}
			continue
		}
		f, decErr := frame.Decode(buf[:n])
		if decErr != nil {
			log.Printf("sink: decode error from %s: %v", src, decErr)
			continue
		}
		cnt := received.Add(1)
		fmt.Printf("recv  src=%-26s  txid[0:4]=%08X  payload_len=%d\n",
			src, binary.BigEndian.Uint32(f.TxID[0:4]), len(f.Payload))
		if limit > 0 && int(cnt) >= limit {
			log.Printf("sink: received %d frames", cnt)
			close(done)
			return
		}
	}
}

// tcpSink accepts TCP connections on 127.0.0.1:port and returns 0 once want
// bytes have arrived across all of them, or 1 on timeout.
func tcpSink(port int, want int64, timeout time.Duration) int {
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		log.Fatalf("listen tcp 127.0.0.1:%d: %v", port, err)
	}
	defer func() { _ = ln.Close() }()
	log.Printf("sink: tcp listening on 127.0.0.1:%d  bytes=%d  timeout=%s", port, want, timeout)

	var got atomic.Int64
	done := make(chan struct{})
	var once sync.Once
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = c.Close() }()
				buf := make([]byte, 65536)
				for {
					n, err := c.Read(buf)
					if n > 0 && got.Add(int64(n)) >= want && want > 0 {
						once.Do(func() { close(done) })
					}
					if err != nil {
						return
					}
				}
			}()
		}
	}()

	var expire <-chan time.Time
	if timeout > 0 {
		expire = time.After(timeout)
	}
	select {
	case <-done:
		log.Printf("sink: received %d bytes", got.Load())
		return 0
	case <-expire:
		log.Printf("sink: TIMEOUT after %s: only received %d/%d bytes", timeout, got.Load(), want)
		return 1
	}
}
