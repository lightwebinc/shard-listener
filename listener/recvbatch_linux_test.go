package listener

import (
	"encoding/binary"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// mmsghdr must match the kernel's struct mmsghdr exactly, or recvmmsg writes
// lengths into the wrong place.
func TestMmsghdrLayout(t *testing.T) {
	if got, want := unsafe.Sizeof(mmsghdr{}), uintptr(unix.SizeofMsghdr+8); got != want {
		t.Fatalf("sizeof mmsghdr = %d, want %d", got, want)
	}
	if off := unsafe.Offsetof(mmsghdr{}.Len); off != unix.SizeofMsghdr {
		t.Fatalf("offsetof Len = %d, want %d", off, unix.SizeofMsghdr)
	}
}

func TestRecvBatchSockaddr(t *testing.T) {
	b := newRecvBatch()
	b.names[0] = unix.RawSockaddrInet6{Family: unix.AF_INET6, Port: 0x3930 /* 12345 big-endian */, Scope_id: 7}
	b.names[0].Addr[15] = 1
	sa, ok := b.sockaddr(0).(*unix.SockaddrInet6)
	if !ok || sa.Port != 12345 || sa.ZoneId != 7 || sa.Addr[15] != 1 {
		t.Fatalf("sockaddr = %#v", sa)
	}
}

// setCtl writes a UDP_GRO control message announcing seg-byte datagrams into
// message i, as the kernel does for a coalesced run.
func setCtl(b *recvBatch, i, seg int) {
	h := (*unix.Cmsghdr)(unsafe.Pointer(&b.ctl[i][0]))
	h.Level, h.Type = unix.IPPROTO_UDP, unix.UDP_GRO
	h.SetLen(unix.SizeofCmsghdr + 4)
	binary.NativeEndian.PutUint32(b.ctl[i][unix.SizeofCmsghdr:], uint32(seg))
	b.hdrs[i].Hdr.SetControllen(unix.CmsgSpace(4))
}

// TestRecvBatchMessageSplit: a message with a UDP_GRO control message is a
// run of seg-byte datagrams with a shorter tail; one without is a single
// datagram, and so is one whose announced size covers the whole message.
func TestRecvBatchMessageSplit(t *testing.T) {
	b := newRecvBatch()
	for i := range b.hdrs {
		b.hdrs[i].Hdr.SetControllen(0)
	}

	b.hdrs[0].Len = 2500
	setCtl(b, 0, 1000)
	if msg, seg := b.message(0); len(msg) != 2500 || seg != 1000 {
		t.Fatalf("coalesced run: len %d seg %d, want 2500 and 1000", len(msg), seg)
	}

	b.hdrs[1].Len = 900 // no control message
	if msg, seg := b.message(1); len(msg) != 900 || seg != 0 {
		t.Fatalf("single datagram: len %d seg %d, want 900 and 0", len(msg), seg)
	}

	b.hdrs[2].Len = 700
	setCtl(b, 2, 700) // a run of one
	if msg, seg := b.message(2); len(msg) != 700 || seg != 0 {
		t.Fatalf("run of one: len %d seg %d, want 700 and 0", len(msg), seg)
	}

	if msg, _ := b.message(3); msg != nil {
		t.Fatal("an empty message must yield nothing")
	}
}

// TestRecvGROLoopback sends one GSO message of five 1000-byte datagrams and
// a 300-byte tail over loopback to a socket with receive GRO on, and reads
// them back through recvBatch: every datagram, whole and in order, however
// the kernel chose to hand them over.
func TestRecvGROLoopback(t *testing.T) {
	rfd, err := unix.Socket(unix.AF_INET6, unix.SOCK_DGRAM, 0)
	if err != nil {
		t.Skip("no IPv6 datagram socket:", err)
	}
	defer func() { _ = unix.Close(rfd) }()
	lo := [16]byte{15: 1}
	if err := unix.Bind(rfd, &unix.SockaddrInet6{Addr: lo}); err != nil {
		t.Skip("cannot bind ::1:", err)
	}
	if err := enableGRO(rfd); err != nil {
		t.Skip("kernel has no UDP_GRO:", err)
	}
	tv := unix.NsecToTimeval((500 * time.Millisecond).Nanoseconds())
	_ = unix.SetsockoptTimeval(rfd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &tv)
	sa, err := unix.Getsockname(rfd)
	if err != nil {
		t.Fatal(err)
	}

	sfd, err := unix.Socket(unix.AF_INET6, unix.SOCK_DGRAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(sfd) }()
	const seg = 1000
	payload := make([]byte, 5*seg+300)
	for i := range payload {
		payload[i] = byte(i / seg) // each datagram carries its index
	}
	oob := make([]byte, unix.CmsgSpace(2))
	h := (*unix.Cmsghdr)(unsafe.Pointer(&oob[0]))
	h.Level, h.Type = unix.IPPROTO_UDP, unix.UDP_SEGMENT
	h.SetLen(unix.CmsgLen(2))
	binary.NativeEndian.PutUint16(oob[unix.CmsgLen(0):], seg)
	if err := unix.Sendmsg(sfd, payload, oob, sa, 0); err != nil {
		t.Skip("kernel refused UDP_SEGMENT:", err)
	}

	b := newRecvBatch()
	var got [][]byte
	coalesced := false
	for total := 0; total < len(payload); {
		n, err := b.recv(rfd)
		if err != nil {
			t.Fatalf("recv after %d of %d bytes: %v", total, len(payload), err)
		}
		for i := 0; i < n; i++ {
			msg, s := b.message(i)
			if s > 0 {
				coalesced = true
				for len(msg) > s {
					got = append(got, append([]byte(nil), msg[:s]...))
					total += s
					msg = msg[s:]
				}
			}
			got = append(got, append([]byte(nil), msg...))
			total += len(msg)
		}
	}
	t.Logf("kernel coalesced the run: %v", coalesced)
	if len(got) != 6 {
		t.Fatalf("%d datagrams, want 6", len(got))
	}
	for i, d := range got {
		want := seg
		if i == 5 {
			want = 300
		}
		if len(d) != want || d[0] != byte(i) || d[len(d)-1] != byte(i) {
			t.Fatalf("datagram %d: %d bytes, first %d last %d", i, len(d), d[0], d[len(d)-1])
		}
	}
}
