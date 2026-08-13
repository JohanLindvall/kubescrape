package otlpexport

// The raw send path hands the SPOOLED BYTES to the wire, and those bytes are
// the diskqueue Reader's own scratch buffer: unmarshalBytes is the identity
// codec, so a reserved payload aliases memory the reader reuses on its very
// next read (and rewrites in place on Requeue). A transport can still be
// referencing a body after a FAILED attempt has returned — net/http's writeLoop
// and grpc's loopyWriter both finish asynchronously once Do/Invoke has errored —
// so the drain copies once per batch before handing it over.
//
// That copy reads like an allocation nicety (its comment is about allocation
// counts), and nothing failed when it was removed: the whole package suite
// passed, under -race included. This is the test that fails.

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
)

// A failed raw attempt leaves the transport holding the slice it was given. The
// drain then reserves the NEXT batch, which the reader decodes into the same
// scratch array — so without the per-batch copy the in-flight body silently
// becomes the tail of a DIFFERENT batch: a spliced protobuf the collector
// rejects, or worse partially accepts, with no counter telling it apart from an
// ordinary rejection.
func TestRawSendGetsAPrivateCopyOfTheReservedPayload(t *testing.T) {
	// Same shape, same LENGTH, different content: the clobber is an in-place
	// overwrite of the reader's scratch, so equal lengths are what make it a
	// pure content change rather than a reallocation that would hide it.
	first := logsWith(strings.Repeat("A", 512))
	second := logsWith(strings.Repeat("B", 512))
	want, err := (&plog.ProtoMarshaler{}).MarshalLogs(first)
	if err != nil {
		t.Fatal(err)
	}

	var (
		retained []byte // what the "transport" is still writing, kept by REFERENCE
		sends    int
	)
	s, _ := countingSink(t, t.TempDir(), 1<<20, func(_ context.Context, data []byte) error {
		sends++
		if sends == 1 {
			// Deliberately NOT copied: this stands in for the bytes grpc's
			// loopyWriter is still flushing when Invoke returns an error.
			retained = data
			return errors.New("connection reset by peer") // transient: the batch is retried
		}
		return nil
	})
	if err := s.enqueue(first); err != nil {
		t.Fatal(err)
	}
	if err := s.enqueue(second); err != nil {
		t.Fatal(err)
	}
	s.drainUntilEmpty(context.Background())

	if sends < 3 {
		t.Fatalf("the drain made %d raw sends, want the retry of the first batch plus the second (>= 3): the aliasing window never opened", sends)
	}
	if len(retained) != len(want) {
		t.Fatalf("retained %d bytes, want %d", len(retained), len(want))
	}
	if !bytes.Equal(retained, want) {
		t.Fatalf("the payload handed to a failed send was rewritten under the transport: it now carries %d bytes of another batch. "+
			"The drain must hand the wire a private copy — the reserved slice is the diskqueue reader's scratch, and the next Reserve (or a Requeue) overwrites it in place",
			len(retained))
	}
}
