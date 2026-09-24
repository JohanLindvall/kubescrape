package otlpingest

import (
	"context"

	grpcpeer "google.golang.org/grpc/peer"

	"github.com/JohanLindvall/kubescrape/internal/peerip"
)

// The connection's peer IP travels on the context from the transport handlers
// to the enricher's opt-in fallback attribution.
//
// The transports stamp it ONLY when that fallback is on (Server.stampPeer):
// peerAttrs is its one reader, behind Config.PeerIPFallback, and the stamp is
// not free — five allocations per gRPC push (the address rendered, parsed and
// boxed into a context value), two per HTTP one — so every push of the
// default configuration paid for a value nothing read.
type peerIPCtxKey struct{}

func withPeerIP(ctx context.Context, hostport string) context.Context {
	// peerip.From is shared with the metadata service's /v1/self
	// attribution: both look the result up in the same pod-IP index, so both
	// must canonicalise it the same way (zone stripped, 4-in-6 unmapped).
	host := peerip.From(hostport)
	if host == "" {
		return ctx
	}
	return context.WithValue(ctx, peerIPCtxKey{}, host)
}

// grpcPeerCtx stamps the gRPC connection's peer address onto the context.
func grpcPeerCtx(ctx context.Context) context.Context {
	return withPeerIP(ctx, grpcPeerAddr(ctx))
}

// grpcPeerAddr is the gRPC connection's peer address, or "" — the one
// extraction, shared by the stamp above and by the lines that name a sender
// (a refusal, a failed forward). p.Addr.String() allocates, so it runs only
// where its result is read: on those paths, and for the stamp only when the
// peer-IP fallback is on.
func grpcPeerAddr(ctx context.Context) string {
	if p, ok := grpcpeer.FromContext(ctx); ok && p.Addr != nil {
		return p.Addr.String()
	}
	return ""
}

// peerIP returns the peer IP recorded on the context, or "".
func peerIP(ctx context.Context) string {
	ip, _ := ctx.Value(peerIPCtxKey{}).(string)
	return ip
}
