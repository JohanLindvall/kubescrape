package main

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/tap"

	"github.com/JohanLindvall/kubescrape/internal/bearer"
)

// The internal hop's auth tap runs on grpc-go's per-connection frame-read
// goroutine with the transport's mutex held, so it must never block on the
// token mount. It used to reach bearer.Rotating.Tokens, which re-reads the file
// on its caller once the refresh interval has lapsed — and on a wedged CSI/NFS
// projection that read never returns, freezing every stream on the sibling's
// connection until MaxConnectionAge reaped it.
//
// The wedge is a FIFO with no writer at the token path (open(2) itself hangs),
// the same shape internal/bearer's own in-flight test uses, and the clock is
// stepped past the refresh deadline so a refresh is due on the very call.
func TestServiceGraphAuthTapNeverReadsTheTokenFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("s3cr3t\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	now := time.Now()
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	log := slog.New(slog.DiscardHandler)
	tok, err := bearer.NewRotating(path, log, bearer.WithClock(clock))
	if err != nil {
		t.Fatal(err)
	}
	// Swap the file for a FIFO only AFTER the initial (fatal) read succeeded.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Skipf("mkfifo unsupported here: %v", err)
	}
	// Release any reader a regression left parked in open(2): opening the write
	// end completes the reader's open, and closing it gives the read an EOF.
	t.Cleanup(func() {
		if f, err := os.OpenFile(path, os.O_WRONLY|syscall.O_NONBLOCK, 0); err == nil {
			_ = f.Close()
		}
	})
	mu.Lock()
	now = now.Add(10 * time.Second) // far past bearer.DefaultRefreshInterval
	mu.Unlock()

	rcv := &sgReceiver{tokens: tok.Tokens, cached: tok.Cached, log: log}
	type verdict struct {
		ctx context.Context
		err error
	}
	done := make(chan verdict, 1)
	go func() {
		info := &tap.Info{Header: metadata.Pairs("authorization", "Bearer s3cr3t")}
		ctx, err := rcv.authTap(context.Background(), info)
		done <- verdict{ctx, err}
	}()
	select {
	case v := <-done:
		if v.err != nil || v.ctx == nil {
			t.Fatalf("authTap refused the current token while the mount was wedged: ctx=%v err=%v", v.ctx, v.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("authTap blocked on the token file: the gRPC tap must answer from the cached accept set and never read the mount on the transport's goroutine")
	}

	// Returning promptly is not enough: Tokens bounds its own wait (it parks
	// the read on another goroutine after a short grace), so a tap that still
	// CLAIMED the refresh would return in time and pay that grace on the
	// transport goroutine every refresh interval. The claim itself is the
	// defect, and it is visible here: a read that started is parked in open(2)
	// on the FIFO, and a non-blocking open of the write end succeeds exactly
	// when a reader is there (ENXIO otherwise). Polled, because the claimed
	// read runs on its own goroutine.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if f, err := os.OpenFile(path, os.O_WRONLY|syscall.O_NONBLOCK, 0); err == nil {
			_ = f.Close()
			t.Fatal("authTap started a read of the token file: the tap must read the cached set (Rotating.Cached) and leave the refresh to Rotating.Run")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
