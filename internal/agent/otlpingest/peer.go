package otlpingest

import (
	"context"
	"net"
	"strings"

	grpcpeer "google.golang.org/grpc/peer"
)

// The connection's peer IP travels on the context from the transport handlers
// to the enricher's opt-in fallback attribution.
type peerIPCtxKey struct{}

func withPeerIP(ctx context.Context, hostport string) context.Context {
	host, _, err := net.SplitHostPort(hostport)
	if err != nil {
		host = hostport // no port (e.g. a unix socket peer won't parse anyway)
	}
	// A link-local peer arrives zone-scoped ("fe80::1%eth0"); the store keys
	// bare IPs, so strip the zone before parsing/stamping.
	if i := strings.IndexByte(host, '%'); i >= 0 {
		host = host[:i]
	}
	if net.ParseIP(host) == nil {
		return ctx
	}
	return context.WithValue(ctx, peerIPCtxKey{}, host)
}

// grpcPeerCtx stamps the gRPC connection's peer address onto the context.
func grpcPeerCtx(ctx context.Context) context.Context {
	if p, ok := grpcpeer.FromContext(ctx); ok && p.Addr != nil {
		return withPeerIP(ctx, p.Addr.String())
	}
	return ctx
}

// peerIP returns the peer IP recorded on the context, or "".
func peerIP(ctx context.Context) string {
	ip, _ := ctx.Value(peerIPCtxKey{}).(string)
	return ip
}
