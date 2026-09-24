package servicemonitors

// The /v1/scrape-auth allowlist: the secret references indexed monitors name,
// memoised on the Index's change token and handed out read-only.

// AuthRefs is the allowlist AuthSecretRefs answers with: a READ-ONLY view of
// the memoised "namespace/name/key" set.
//
// A bare map is the obvious return type and was the previous one. It is the
// wrong one HERE, because what is returned is not a value the caller owns: it
// is the memo itself, shared by every concurrent /v1/scrape-auth request until
// the next monitor change, and it is the boundary that keeps
// -scrape-auth-secrets from being a general secret-read API. One entry written
// into it widens what the service is willing to read, cluster-wide, with
// nothing anywhere to notice. Handing out a COPY instead would put an
// allocation back on the route the memo exists to keep free — a maps.Clone of
// the 400 refs BenchmarkAuthSecretRefs builds is 13,656 B and 4 allocations per
// request, against 0 for this view — so the safety is structural rather than a
// copy or a comment: the set has no exported field and no method that can add
// to it, so a caller outside this package CANNOT widen it. It is not trusted
// not to.
type AuthRefs struct{ refs map[string]struct{} }

// Has reports whether ref — the "namespace/name/key" join the scrape-auth
// route builds from its three path segments — is allowlisted.
func (a AuthRefs) Has(ref string) bool {
	_, ok := a.refs[ref]
	return ok
}

// Len is the number of allowlisted references.
func (a AuthRefs) Len() int { return len(a.refs) }

// AuthSecretRefs returns the set of "namespace/name/key" references named by
// any secret-bearing field of any indexed ServiceMonitor or PodMonitor
// endpoint — every field Endpoint.secretRefs lists: the bearer token, the
// basicAuth username and password, the authorization credentials, and the
// tlsConfig CA, client certificate and private key. The scrape-auth endpoint
// serves ONLY these, so a direct HTTP caller cannot use it to read arbitrary
// cluster secrets — only the secret keys a monitor actually references. Those
// include TLS private keys, not just tokens, which is part of why the route is
// authenticated.
//
// THE RESULT IS THE SHARED MEMO, not a copy — see AuthRefs for why that is a
// type and not a comment. (server.monitoredServices carries the same shared
// contract by convention; its consumer is inside this repo's own request path,
// and what it holds is not a security boundary.)
//
// Memoised on the change token, because this is the allowlist check on the ONE
// route holding cluster-wide `secrets: get` and every agent re-asks each
// credential once a minute: rebuilding it per request was O(monitors ×
// endpoints) of pure garbage under the index's read lock, which the informer's
// writes contend with. Measured (BenchmarkAuthSecretRefs, 200 ServiceMonitors +
// 200 PodMonitors of one secret-bearing endpoint each): 27,112 B and 15
// allocations per request, scaling with the REFS — an endpoint that also names
// a tlsConfig ca/cert/keySecret is four of them — against 0 allocations once
// the answer is held.
func (ix *Index) AuthSecretRefs() AuthRefs {
	// Read the token BEFORE the build, exactly as server.monitoredServices
	// does: a mutation landing during the harvest is then recorded as unbuilt
	// and rebuilds on the next call, rather than being stamped as
	// already-included and lost until some unrelated change moves the token.
	// Getting this backwards on THIS map would leave a removed monitor's secret
	// reachable.
	gen := ix.Generation()
	ix.authMu.Lock()
	defer ix.authMu.Unlock()
	if !ix.authValid || ix.authGen != gen {
		ix.authRefs = ix.buildAuthSecretRefs()
		ix.authGen, ix.authValid = gen, true
	}
	return ix.authRefs
}

// buildAuthSecretRefs harvests the allowlist from scratch.
func (ix *Index) buildAuthSecretRefs() AuthRefs {
	ix.authBuilds.Add(1)
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	out := map[string]struct{}{}
	add := func(eps []Endpoint) {
		for i := range eps {
			// Every secret an endpoint references, so the metadata service
			// serves exactly the keys some monitor actually names — the
			// allowlist that keeps -scrape-auth-secrets from being a general
			// secret-read API. The FIELD LIST is Endpoint.secretRefs, the same
			// one both parsers namespace with: an allowlist that could disagree
			// with the namespacing is a 404 on one side or an unmatchable ref on
			// the other, and both scrape unauthenticated.
			//
			// Indexed, not ranged by value: secretRefs takes the address of the
			// endpoint's fields, and a loop copy's addresses are the copy's.
			for _, ref := range eps[i].secretRefs() {
				if *ref != "" {
					out[*ref] = struct{}{}
				}
			}
		}
	}
	for _, m := range ix.monitors {
		add(m.Endpoints)
	}
	for _, m := range ix.podMonitors {
		add(m.Endpoints)
	}
	return AuthRefs{refs: out}
}
