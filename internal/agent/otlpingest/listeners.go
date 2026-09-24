package otlpingest

// The run/shutdown skeleton shared by kubescrape's OTLP receivers.
//
// There are two of them: this package's Server (the agent's application-facing
// ingest listeners and the trace tier's application ports) and the trace
// tier's authenticated internal receiver (sgReceiver in
// cmd/kubescrape-agent/sgreceiver.go). The skeleton — bind explicitly, THEN
// signal readiness, serve until the context or a listener failure, then stop
// both gracefully under one bounded drain — is receiver-independent, and having
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
// decode window (reserveWindowFor). The age bounds are the coarse complement:
// whatever a peer accumulates on one socket (stream ids, HPACK state,
// half-open RPCs), it gets a GOAWAY after the age and loses the socket after
// the grace. Conformant senders reconnect transparently.
//
// The 30s grace is NOT longer than every RPC here. At the default message cap
// the decode window is 10s, but reserveWindowFor scales it with
// -ingest-grpc-max-recv-bytes up to five minutes (past 30s from roughly 12 MiB),
// so an upload still in flight when its connection ages out can be cut by the
// grace. Nothing is lost: the push was never acked, the sender sees the stream
// fail with a retryable code and re-sends it on a fresh connection. Deriving
// the grace from the window was rejected because this option is shared with
// the trace tier's internal receiver, which has no decode window at all, and
// the cost is one retried push per aged-out connection every 30 minutes.
func KeepaliveOption() grpc.ServerOption {
	return grpc.KeepaliveParams(keepalive.ServerParameters{
		MaxConnectionIdle:     120 * time.Second,
		MaxConnectionAge:      30 * time.Minute,
		MaxConnectionAgeGrace: 30 * time.Second,
	})
}

// maxHeaderListBytes bounds one request's header block on every kubescrape
// OTLP receiver, BOTH transports: the HPACK-decoded size on the gRPC arm
// (MaxHeaderListSizeOption) and the wire size of the request line plus headers
// on the HTTP arm (NewPushHTTPServer's MaxHeaderBytes, where net/http's default
// is 1 MiB). One constant, so the two arms of one listener cannot drift apart.
//
// grpc-go's server default is 16 MiB, and nothing in this repo was lowering it.
// On the listeners this repo documents as unauthenticated — the agent's
// -ingest :4317 and the trace tier's application ports — that is 16 MiB a peer
// can make the process decode per stream before any application code runs, and
// it is worse than an allocation: grpc-go renders a header it cannot decode
// into its own log line (internal/transport's "Failed to decode metadata
// header (%q, %q)"), so the same bytes were an amplifier too. internal/cli's
// grpclog adapter closes the logging half by clipping and by keeping that class
// at Debug; this closes the RECEIVE half, which is the one that exists whatever
// the adapter does with the message.
//
// 64 KiB is many times the largest header set any OTLP sender writes (a bearer
// token, a tenant id, the h2 pseudo-headers), so no legitimate push is refused;
// the internal hop's own token rides in the same budget. A sender past it gets
// a clean protocol-level refusal rather than a mystery stream reset, because
// grpc-go advertises the value in its SETTINGS frame and a conformant client
// checks against it before writing the headers.
const maxHeaderListBytes = 64 << 10

// MaxHeaderListSizeOption is that bound as a grpc.ServerOption, exported for
// the trace tier's internal receiver, which assembles its own grpc.Server.
func MaxHeaderListSizeOption() grpc.ServerOption {
	return grpc.MaxHeaderListSize(maxHeaderListBytes)
}

// httpShutdownGrace bounds a listener shutdown — BOTH transports, one clock:
// how long in-flight pushes get before Run force-closes their connections. The
// force-close that follows an expired grace is load-bearing on each side:
// http.Server.Shutdown NEVER interrupts an active handler, and
// NewPushHTTPServer's ReadTimeout allows a request body 60 seconds of trickle,
// while grpc.Server.GracefulStop waits for every pending RPC however long its
// forward takes — so without it a straggling push could finish, and be acked,
// long after the rest of the process had flushed and stopped. (The name is the
// HTTP side's, which had the bound first.)
const httpShutdownGrace = 5 * time.Second

// NewPushHTTPServer returns the *http.Server shape for an OTLP/HTTP push
// listener. ReadHeaderTimeout kills Slowloris header trickling; ReadTimeout
// bounds a trickled request body (the handlers read up to 16 MiB and senders
// are node-local, so 60s is generous). It governs READING the request — body
// included — and nothing after: a handler that has finished the read runs on
// its own clock, and what bounds the forward is the exporter's own timeout.
// IdleTimeout reaps parked keep-alives. WriteTimeout is deliberately
// omitted: responses are tiny and its clock would race a slow-but-legal body
// upload plus the forward.
//
// MaxHeaderBytes is the gRPC arm's header bound (maxHeaderListBytes): net/http's
// 1 MiB default let an unauthenticated peer make a handler parked in its body
// read retain ~3 MiB of parsed header for the whole ReadTimeout, charged to no
// admission budget. A head over the bound is refused 431 by net/http before any
// handler runs. It bounds the cost PER CONNECTION, not in total: the parsed
// header still costs ~3x its wire size while the body is read, and the
// connection count is uncapped (docs/CONFIGURATION.md, Accepted security
// residuals).
func NewPushHTTPServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    maxHeaderListBytes,
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

	// shutdownGrace overrides httpShutdownGrace (tests only; zero = the
	// default). Injectable so the grace-expired-then-force-close paths are
	// provable without a five-second test.
	shutdownGrace time.Duration
}

// Run serves until ctx is cancelled, then shuts both listeners down together
// under one grace (shutdown). A runtime listener failure propagates as an
// error — callers treat it as fatal — while a ctx-cancelled shutdown returns
// nil.
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
	l.shutdown(log)
	return runErr
}

// shutdown stops both listeners TOGETHER, under one grace.
//
// It used to run gRPC's GracefulStop to completion — unbounded, for as long as
// any pushed RPC's forward took — and only then begin the HTTP Shutdown. So
// after the process was told to stop, the HTTP listener went on ACCEPTING new
// pushes for as long as one slow gRPC export lasted (measured: a push accepted
// and acked 4.5 s into the shutdown), which is exactly the window in which the
// rest of the process is flushing its final exports: an ack the flush no longer
// covers. Now both doors close at once — Shutdown and GracefulStop each stop
// accepting immediately — and both drains share httpShutdownGrace. Whatever is
// still in flight when it expires is force-closed on either transport (Close,
// Stop): its sender sees a transport error and retries against the replacement
// pod, which is at-least-once — better a NACK than an ack nothing downstream
// will honour.
func (l Listeners) shutdown(log *slog.Logger) {
	grace := l.shutdownGrace
	if grace <= 0 {
		grace = httpShutdownGrace
	}
	sctx, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()

	var grpcDone chan struct{}
	if l.GRPC != nil {
		grpcDone = make(chan struct{})
		go func() {
			l.GRPC.GracefulStop()
			close(grpcDone)
		}()
	}
	if l.HTTP != nil {
		if err := l.HTTP.Shutdown(sctx); err != nil {
			// The grace expired with requests still in flight (Shutdown never
			// interrupts an active handler, and ReadTimeout gives a trickled
			// body 60s). A handler still running past the grace now answers
			// into a closed connection.
			//
			// Said out loud because from the SENDER's side this is an
			// unexplained transport failure at exactly the moment a rolling
			// update is being blamed for something, and it happens at most once
			// per process — there is no flood to throttle.
			log.Warn(l.Name+": the HTTP listener still had pushes in flight when the shutdown grace expired; "+
				"their connections are closed and the senders will retry against the replacement pod",
				"addr", l.HTTP.Addr, "grace", grace, "error", err)
			_ = l.HTTP.Close()
		}
	}
	if grpcDone != nil {
		select {
		case <-grpcDone:
		case <-sctx.Done():
			// The gRPC mirror of the HTTP Close above: Stop closes every
			// connection and cancels the RPCs still running, and GracefulStop,
			// still waiting on them, returns.
			log.Warn(l.Name+": the gRPC listener still had pushes in flight when the shutdown grace expired; "+
				"their connections are closed and the senders will retry against the replacement pod",
				"addr", l.GRPCAddr, "grace", grace)
			l.GRPC.Stop()
			<-grpcDone
		}
	}
}
