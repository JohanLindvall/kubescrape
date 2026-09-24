package otlpexport

import (
	"net/http"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// OTLP defines 408/429/5xx as RETRYABLE. A collector answering them while it
// accepts other batches is back-pressuring, not rejecting this payload — so
// they must not count as poison evidence and get a good batch dropped.
func TestProbeThrottlingIsNotPoisonEvidence(t *testing.T) {
	for _, code := range []int{408, 429, 500, 502, 503, 504} {
		if respondedError(&HTTPStatusError{Code: code}) {
			t.Errorf("HTTP %d counted as poison evidence; it is retryable back-pressure", code)
		}
	}
	// A genuine rejection still counts.
	for _, code := range []int{400, 401, 403, 404, 413, 422} {
		if !respondedError(&HTTPStatusError{Code: code}) {
			t.Errorf("HTTP %d must count as a collector response", code)
		}
	}
	_ = http.StatusOK
}

// The gRPC spelling of the same rule. OTLP's retryable gRPC statuses include a
// ResourceExhausted that carries RetryInfo — the gRPC form of HTTP 429 — plus
// Aborted, OutOfRange and DataLoss, and the poison gate used to exclude only
// Unavailable/DeadlineExceeded/Canceled: under partial gRPC throttling a good
// batch spent its poison budget and was dropped, where the identical throttle
// over HTTP never counted. A BARE ResourceExhausted (grpc-go's over-limit
// refusal of this message) is the poison the budget exists for, and still
// counts.
func TestGRPCThrottlingIsNotPoisonEvidence(t *testing.T) {
	throttled, err := status.New(codes.ResourceExhausted, "collector overloaded").
		WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	for name, e := range map[string]error{
		"resource exhausted + RetryInfo": throttled.Err(),
		"aborted":                        status.Error(codes.Aborted, "conflict"),
		"out of range":                   status.Error(codes.OutOfRange, "seek past end"),
		"data loss":                      status.Error(codes.DataLoss, "partial write"),
	} {
		if respondedError(e) {
			t.Errorf("gRPC %s counted as poison evidence; OTLP defines it as retryable back-pressure", name)
		}
	}
	if !respondedError(status.Error(codes.ResourceExhausted, "grpc: received message larger than max")) {
		t.Error("a bare ResourceExhausted (the over-limit refusal of THIS payload) must still count as poison evidence")
	}
}
