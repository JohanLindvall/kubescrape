package server

// GET /debug: the metadata service's debug homepage. The agent has one
// (cmd/kubescrape-agent/debughome.go, plain links — its endpoints take no
// parameters); this service's most useful debug surface is /v1/explain,
// which takes a namespace and a pod name, so the homepage carries small
// FORMS that navigate to the parameterised routes rather than bare links.
// Served unconditionally — no readiness gate: a service that is NOT ready is
// exactly when someone reaches for the debug page (the routes themselves
// still gate).

import "net/http"

func (s *Server) handleDebugHome(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(debugHomeHTML))
}

const debugHomeHTML = `<!doctype html>
<meta charset="utf-8">
<title>kubescrape debug</title>
<style>
  body { font: 14px/1.5 system-ui, sans-serif; margin: 1.5rem; max-width: 46rem; }
  fieldset { border: 1px solid #ccc; border-radius: 6px; margin-bottom: .75rem; }
  input[type=text] { font: inherit; padding: .2rem .4rem; margin-right: .4rem; }
  button { font: inherit; padding: .25rem .9rem; margin-right: .4rem; }
  code, a { font-family: ui-monospace, monospace; }
  .d { color: #555; }
  .n { color: #777; font-size: 13px; }
  li { margin-bottom: .4rem; }
</style>
<h1>kubescrape debug</h1>
<ul>
  <li><a href="/healthz">/healthz</a> — <span class="d">liveness (static ok)</span></li>
  <li><a href="/readyz">/readyz</a> — <span class="d">readiness (informer caches + handler registrations synced)</span></li>
  <li><a href="/v1/self">/v1/self</a> — <span class="d">the pod owning YOUR connection's source address (404 from outside the pod network)</span></li>
</ul>

<fieldset><legend>Why is this pod (not) scraped?</legend>
  <form onsubmit="return go('/v1/explain/' + enc('ns') + '/' + enc('pod'))">
    <input type="text" id="ns" placeholder="namespace" required>
    <input type="text" id="pod" placeholder="pod name" required>
    <button>Explain</button>
    <button type="button" onclick="go('/v1/pods/' + enc('ns') + '/' + enc('pod'))">Pod metadata</button>
  </form>
  <p class="d">The whole decision chain, verdict by verdict: scrapeable
  reasons, per-entry port resolution, Service and monitor matches, the final
  merged targets.</p>
</fieldset>

<fieldset><legend>Node</legend>
  <form onsubmit="return go('/v1/nodes/' + enc('node') + '/targets')">
    <input type="text" id="node" placeholder="node name" required>
    <button>Scrape targets</button>
    <button type="button" onclick="go('/v1/nodes/' + enc('node') + '/metadata')">Node metadata</button>
  </form>
</fieldset>

<fieldset><legend>Lookups</legend>
  <form onsubmit="return go('/v1/containers/' + enc('cid'))">
    <input type="text" id="cid" placeholder="container id (any runtime prefix ok)" size="34" required>
    <button>Container</button>
  </form>
  <form onsubmit="return go('/v1/pod-uids/' + enc('uid'))">
    <input type="text" id="uid" placeholder="pod uid" size="34" required>
    <button>Pod by UID</button>
  </form>
  <form onsubmit="return go('/v1/pod-ips/' + enc('ip'))">
    <input type="text" id="ip" placeholder="pod ip" size="34" required>
    <button>Pod by IP</button>
  </form>
</fieldset>

<p class="n">Prometheus metrics (<code>/metrics</code>) and pprof
(<code>/debug/pprof/</code>) live on their own ports (<code>-metrics-listen</code>,
<code>-pprof-listen</code>). <code>/v1/scrape-auth</code> is the one
authenticated route and is opt-in (<code>-scrape-auth-secrets</code>). The
per-node agents each serve their own debug homepage on their
<code>-listen</code> port (<code>:8081</code>): tail positions, per-target
scrape outcomes, and a live stream of what they are exporting.</p>

<script>
"use strict";
function enc(id) { return encodeURIComponent(document.getElementById(id).value.trim()); }
function go(path) { location.href = path; return false; }
</script>
`
