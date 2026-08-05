package otlpingest

// The run/shutdown skeleton shared by kubescrape's OTLP receivers.
//
// There are two of them: this package's Server (the agent's application-facing
// ingest listeners and the trace tier's application ports) and the trace
// tier's authenticated internal receiver (sgReceiver in
// cmd/kubescrape-agent/servicegraph.go). The skeleton — bind explicitly, THEN
// signal readiness, serve until the context or a listener failure, then stop
// gracefully with a bounded HTTP drain — is receiver-independent, and having
// it twice is how the two drifted on the gRPC keepalive policy (the internal
// port never picked up the connection-age bounds). Exported so the sgReceiver
// can adopt it.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// KeepaliveOption is the gRPC server keepalive policy every kubescrape OTLP
// receiver wants, exported so the trace tier's internal port can share it.
//
// It mirrors NewPushHTTPServer's IdleTimeout: reap connections apps opened and
// abandoned (default gRPC keeps them forever). MaxConnectionIdle alone does
// NOT cover the abandoned-STREAM case — a connection carrying an open stream
// is not idle, which is why the ingest Server's reservation carries its own
// window (grpcReserveWindow). The age bounds are the coarse complement:
// whatever a peer accumulates on one socket (stream ids, HPACK state,
// half-open RPCs), it gets a GOAWAY after the age and loses the socket after
// the grace. Conformant senders reconnect transparently, and the grace is far
// longer than any RPC here — those are bounded by the pre-decode window plus
// the exporter's own timeout.
func KeepaliveOption() grpc.ServerOption {
	return grpc.KeepaliveParams(keepalive.ServerParameters{
		MaxConnectionIdle:     120 * time.Second,
		MaxConnectionAge:      30 * time.Minute,
		MaxConnectionAgeGrace: 30 * time.Second,
	})
}

// NewPushHTTPServer returns the *http.Server shape for an OTLP/HTTP push
// listener. ReadHeaderTimeout kills Slowloris header trickling; ReadTimeout
// bounds a trickled request body (the handlers read up to 16 MiB and senders
// are node-local, so 60s is generous). It governs READING the request — body
// included — and nothing after: a handler that has finished the read runs on
// its own clock, and what bounds the forward is the exporter's own timeout.
// IdleTimeout reaps parked keep-alives. WriteTimeout is deliberately
// omitted: responses are tiny and its clock would race a slow-but-legal body
// upload plus the forward.
func NewPushHTTPServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
}

// Listeners runs an optional gRPC server and an optional HTTP server as one
// unit: both bound before Ready fires, either failing at runtime taking the
// pair down, both shut down when ctx is cancelled. Neither configured makes
// Run a no-op (and Ready is then never called — there is nothing to be ready).
type Listeners struct {
	// Name prefixes log lines and errors ("otlp ingest", "service-graph
	// internal").
	Name   string
	Logger *slog.Logger // nil = slog.Default()

	// GRPC is served on GRPCAddr when non-nil. The server is the caller's to
	// build — interceptors, taps and service registrations differ per receiver
	// — but KeepaliveOption is the policy they share.
	GRPC     *grpc.Server
	GRPCAddr string

	// HTTP is served on its own Addr when non-nil (NewPushHTTPServer is the
	// shape push receivers want).
	HTTP *http.Server

	// Ready is called once every configured listener is BOUND (not once
	// something has been received). It is what a readiness gate hangs on: a
	// rollout that advanced on a probe answering before the port existed would
	// march a broken listener across the fleet.
	Ready func()
}

// Run serves until ctx is cancelled, then shuts both listeners down
// (GracefulStop for gRPC; a 5-second-bounded Shutdown for HTTP). A runtime
// listener failure propagates as an error — callers treat it as fatal — while
// a ctx-cancelled shutdown returns nil.
func (l Listeners) Run(ctx context.Context) error {
	log := l.Logger
	if log == nil {
		log = slog.Default()
	}
	errc := make(chan error, 2)
	started := 0

	if l.GRPC != nil {
		lis, err := net.Listen("tcp", l.GRPCAddr)
		if err != nil {
			return fmt.Errorf("%s gRPC listen %s: %w", l.Name, l.GRPCAddr, err)
		}
		started++
		go func() { errc <- l.GRPC.Serve(lis) }()
		log.Info(l.Name+" gRPC listening", "addr", l.GRPCAddr)
	}

	if l.HTTP != nil {
		// Listened explicitly rather than through ListenAndServe so the BIND
		// failure is known before Ready fires below: a readiness probe that goes
		// green on a port nobody bound is worse than no probe.
		lis, err := net.Listen("tcp", l.HTTP.Addr)
		if err != nil {
			if l.GRPC != nil {
				l.GRPC.Stop()
			}
			return fmt.Errorf("%s HTTP listen %s: %w", l.Name, l.HTTP.Addr, err)
		}
		started++
		go func() {
			// ErrServerClosed is the ordinary shutdown, not a failure.
			if err := l.HTTP.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errc <- err
				return
			}
			errc <- nil
		}()
		log.Info(l.Name+" HTTP listening", "addr", l.HTTP.Addr)
	}

	if started == 0 {
		return nil
	}
	if l.Ready != nil {
		l.Ready()
	}

	// A runtime listener failure must propagate to the caller (main treats it
	// as fatal and exits non-zero); a ctx-cancelled shutdown returns nil.
	var runErr error
	select {
	case <-ctx.Done():
	case err := <-errc:
		if err != nil {
			log.Error(l.Name+" listener failed", "error", err)
			runErr = fmt.Errorf("%s listener: %w", l.Name, err)
		}
	}
	if l.GRPC != nil {
		l.GRPC.GracefulStop()
	}
	if l.HTTP != nil {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = l.HTTP.Shutdown(sctx)
	}
	return runErr
}
