package otlpexport

// The transport-vs-destination partition of Config.
//
// Config mixes two kinds of field, and every DERIVED client config in the repo
// has to split them the same way:
//
//   - TRANSPORT fields describe HOW this process talks OTLP — compression,
//     timeouts, the send-size cap, the retry shape. They are properties of the
//     sender and are safe (and wanted) to inherit into a client aimed at a
//     different host.
//   - DESTINATION fields describe WHO is being talked to and what is presented
//     to them — endpoint, TLS material and trust decisions, static headers,
//     the bearer token. These are scoped to ONE destination, and inheriting
//     them is a security bug, not a convenience: presenting the collector's
//     bearer token (or mTLS client certificate, or an X-Scope-OrgID header) to
//     a different host leaks a credential across a trust boundary because a
//     field was left empty.
//
// The two derivations that must build on this partition are
// cmd/kubescrape-agent's routeExportConfig (a route naming its own endpoint
// drops every base credential — each is taken from the route or left unset)
// and servicegraph.ReshardConfig.clientConfig (a shard client inherits only
// the transport tuning; everything destination-scoped comes from the
// serviceGraphShards section). Both used to spell the split by hand;
// TestConfigFieldsAreClassified is what forces a NEW Config field to be
// assigned to one side deliberately instead of drifting into whichever
// derivation forgets it.

// TransportOnly returns only the inheritable transport subset of c, with every
// destination-scoped field zeroed. Callers then fill in the destination —
// endpoint, TLS, headers, credentials — from their own configuration, and may
// override the transport fields they own (the reshard hop pins RetryAttempts
// to 1, for instance).
func (c Config) TransportOnly() Config {
	return Config{
		Protocol:         c.Protocol,
		Compression:      c.Compression,
		CompressionLevel: c.CompressionLevel,
		Timeout:          c.Timeout,
		RetryAttempts:    c.RetryAttempts,
		RetryBackoff:     c.RetryBackoff,
		MaxSendBytes:     c.MaxSendBytes,
	}
}
