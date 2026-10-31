package listener

import (
	"testing"
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
