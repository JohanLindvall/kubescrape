package spool_test

import (
	"fmt"
	"os"

	"github.com/JohanLindvall/kubescrape/pkg/spool"
)

func Example() {
	dir, _ := os.MkdirTemp("", "spool")
	defer func() { _ = os.RemoveAll(dir) }()

	q, err := spool.Open(dir, spool.Options{MaxBytes: 64 << 20})
	if err != nil {
		panic(err)
	}
	defer func() { _ = q.Close() }()

	// Append is durable: once it returns, the record survives a crash.
	if err := q.Append([]byte("batch-1")); err != nil {
		panic(err)
	}

	// Pop hands back the record with a commit function; commit only after the
	// record is truly handled (e.g. the collector acked it) — a crash before
	// commit re-delivers it.
	data, commit, ok, err := q.Pop()
	fmt.Printf("%q ok=%v err=%v\n", data, ok, err)
	commit()
	// Output:
	// "batch-1" ok=true err=<nil>
}
