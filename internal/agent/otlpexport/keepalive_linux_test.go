//go:build linux

package otlpexport

import (
	"context"
	"net"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// tcpUserTimeout is Linux's TCP_USER_TIMEOUT socket option. syscall defines
// it only on some architectures (not amd64), and x/sys is not a direct
// dependency, so it is spelled here.
const tcpUserTimeout = 0x12

// The gRPC exporter's connection must abort a socket whose peer stopped
// acknowledging. grpc-go sets TCP_USER_TIMEOUT only when client keepalive is
// ENABLED, and without it a collector connection that died with no FIN or RST
// — its node lost or partitioned, its netns torn down — kept taking every
// export until the kernel gave up retransmitting (~15 minutes at the default
// tcp_retries2): every RPC failed DeadlineExceeded, nothing re-dialled, and the
// tailer's single sweep goroutine stalled node-wide. Read off a real socket
// dialled with exactly New's options.
func TestGRPCClientSocketAbortsAnUnacknowledgedPeer(t *testing.T) {
	// The ping interval must stay at or above grpc-go's server default
	// EnforcementPolicy.MinTime (5m), or a receiver would answer the pings with
	// a too_many_pings GOAWAY.
	if grpcKeepalive.Time < 5*time.Minute {
		t.Fatalf("keepalive Time = %v, below the 5m server enforcement default", grpcKeepalive.Time)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	dialled := make(chan net.Conn, 1)
	dialer := func(ctx context.Context, addr string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
		if err == nil {
			select {
			case dialled <- conn:
			default:
			}
		}
		return conn, err
	}
	cc, err := grpc.NewClient("passthrough:///"+lis.Addr().String(),
		append(grpcDialOptions(insecure.NewCredentials(), nil), grpc.WithContextDialer(dialer))...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cc.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cc.Connect()
	for s := cc.GetState(); s != connectivity.Ready; s = cc.GetState() {
		if !cc.WaitForStateChange(ctx, s) {
			t.Fatalf("connection never became ready (last state %v)", s)
		}
	}

	var conn net.Conn
	select {
	case conn = <-dialled:
	default:
		t.Fatal("the dialer never produced a connection")
	}
	raw, err := conn.(*net.TCPConn).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var got int
	var gerr error
	if err := raw.Control(func(fd uintptr) {
		got, gerr = syscall.GetsockoptInt(int(fd), syscall.IPPROTO_TCP, tcpUserTimeout)
	}); err != nil {
		t.Fatal(err)
	}
	if gerr != nil {
		t.Fatal(gerr)
	}
	if want := int(grpcKeepalive.Timeout / time.Millisecond); got != want {
		t.Errorf("TCP_USER_TIMEOUT = %dms, want %dms: without client keepalive grpc-go leaves it unset and a silently dead collector holds every export for ~15 minutes", got, want)
	}
}
