// Command sink-test-frames listens on a UDP port and counts incoming BSV
// transaction frames delivered by bitcoin-shard-listener. When -count frames
// have been received it exits 0. If -timeout expires first it exits 1.
//
// Use -raw to count raw UDP datagrams without frame decoding; pass this flag
// when testing strip-header mode where the listener strips the 92-byte BRC-124/BRC-128
// frame header before forwarding.
//
// Usage:
//
//	sink-test-frames [-port N] [-count N] [-raw] [-timeout D]
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/lightwebinc/shard-common/frame"
)

func main() {
	port := flag.Int("port", 9102, "UDP port to listen on")
	count := flag.Int("count", 0, "exit 0 after receiving this many items (0 = run forever)")
	raw := flag.Bool("raw", false, "count raw datagrams without frame decode (for strip-header testing)")
	timeout := flag.Duration("timeout", 30*time.Second, "exit 1 if -count not reached within this duration (0 = no timeout)")
	flag.Parse()

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
