package debugtap

// The HTTP half: GET /debug/otlp (the stream) and GET /debug/otlp/ui (a
// self-contained page driving it). Served on the agent's -listen port beside
// the other /debug endpoints — same exposure profile, and the payloads it
// shows are the ones any pod on the collector path can already see.

import (
	"fmt"
	"math"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"
)

// ServeHTTP streams matching payloads as OTLP JSON Lines until the client
// disconnects.
//
// Query parameters:
//
//	signal  logs | metrics | traces — repeatable; default all three
//	attr    key=valueGlob — repeatable, ANDed; both halves are path.Match
//	        globs over the RESOURCE attributes (attr=k8s.namespace.name=team-*)
//	sample  percentage of matching resources to keep, 0-100 (default 100)
func (t *Tap) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	sig := signal(0)
	for _, s := range q["signal"] {
		switch s {
		case "logs":
			sig |= sigLogs
		case "metrics":
			sig |= sigMetrics
		case "traces":
			sig |= sigTraces
		default:
			http.Error(w, fmt.Sprintf("unknown signal %q (logs|metrics|traces)", s), http.StatusBadRequest)
			return
		}
	}
	if sig == 0 {
		sig = sigAll
	}

	sample := 100.0
	if v := q.Get("sample"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		// The explicit NaN check matters: ParseFloat accepts "NaN", every
		// comparison against it is false, and a NaN sample would stream
		// silence indistinguishable from "no traffic" — the exact failure
		// mode the glob validation below exists to prevent.
		if err != nil || math.IsNaN(f) || f < 0 || f > 100 {
			http.Error(w, fmt.Sprintf("sample %q is not a percentage (0-100)", v), http.StatusBadRequest)
			return
		}
		sample = f
	}

	var filters []attrFilter
	for _, a := range q["attr"] {
		key, val, ok := strings.Cut(a, "=")
		if !ok || key == "" {
			http.Error(w, fmt.Sprintf("attr %q is not key=value", a), http.StatusBadRequest)
			return
		}
		// Validate the globs now: path.Match reports a malformed pattern only
		// when matching, and a filter that silently matches nothing would
		// stream silence indistinguishable from "no traffic".
		for _, g := range []string{key, val} {
			if _, err := path.Match(g, ""); err != nil {
				http.Error(w, fmt.Sprintf("attr %q: bad pattern %q: %v", a, g, err), http.StatusBadRequest)
				return
			}
		}
		filters = append(filters, attrFilter{Key: key, Value: val})
	}

	// Subscribe BEFORE the response header goes out: a refusal must still be
	// able to answer 503, and headers are one-shot.
	sub, unsubscribe := t.subscribe(sig, filters, sample)
	if sub == nil {
		http.Error(w, fmt.Sprintf("too many debug streams (max %d); close one and retry", maxSubscribers),
			http.StatusServiceUnavailable)
		return
	}
	defer unsubscribe()

	// The -listen server's WriteTimeout would kill this stream after 30s;
	// streams own their deadline (none — the client's disconnect ends it).
	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Time{})
	_ = rc.SetReadDeadline(time.Time{})

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(w, "# streaming; signals=%s sample=%g%% filters=%d — one OTLP JSON payload per line\n",
		sigNames(sig), sample, len(filters))
	_ = rc.Flush()
	for {
		select {
		case <-r.Context().Done():
			return
		case b := <-sub.ch:
			sub.queued.Add(int64(-len(b)))
			if _, err := w.Write(append(b, '\n')); err != nil {
				return
			}
			if err := rc.Flush(); err != nil {
				return
			}
			if d := sub.dropped.Swap(0); d > 0 {
				_, _ = fmt.Fprintf(w, "# %d payload(s) dropped for this stream (slow reader)\n", d)
				_ = rc.Flush()
			}
		}
	}
}

func sigNames(s signal) string {
	var names []string
	if s&sigLogs != 0 {
		names = append(names, "logs")
	}
	if s&sigMetrics != 0 {
		names = append(names, "metrics")
	}
	if s&sigTraces != 0 {
		names = append(names, "traces")
	}
	return strings.Join(names, ",")
}

// ServeUI serves the built-in page. Self-contained (inline CSS/JS, no
// external assets): the port it lives on is cluster-internal.
func (t *Tap) ServeUI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(uiHTML))
}

const uiHTML = `<!doctype html>
<meta charset="utf-8">
<title>kubescrape OTLP debug</title>
<style>
  body { font: 14px/1.4 system-ui, sans-serif; margin: 1.5rem; max-width: 72rem; }
  fieldset { border: 1px solid #ccc; border-radius: 6px; margin-bottom: .75rem; }
  label { margin-right: 1rem; }
  input[type=text], input[type=number] { font: inherit; padding: .2rem .4rem; }
  #attrs { width: 28rem; }
  button { font: inherit; padding: .3rem 1rem; margin-right: .5rem; }
  #out { background: #111; color: #ddd; padding: .75rem; border-radius: 6px;
         min-height: 8rem; max-height: 34rem; overflow: auto;
         font: 12px/1.35 ui-monospace, monospace; white-space: pre-wrap;
         word-break: break-all; }
  #status { color: #666; margin-left: .5rem; }
</style>
<h1>OTLP debug stream</h1>
<p>Streams what this agent is exporting (post-transform), as OTLP JSON — the
on-demand counterpart of a collector debug exporter. Raw endpoint:
<code>GET /debug/otlp?signal=logs&amp;attr=k8s.namespace.name=team-*&amp;sample=10</code></p>
<fieldset><legend>Signals</legend>
  <label><input type="checkbox" id="sig-logs" checked> logs</label>
  <label><input type="checkbox" id="sig-metrics" checked> metrics</label>
  <label><input type="checkbox" id="sig-traces" checked> traces</label>
</fieldset>
<fieldset><legend>Resource attribute filters (key=value, one per line; * and ? are wildcards on both halves; ANDed)</legend>
  <textarea id="attrs" rows="3" placeholder="k8s.namespace.name=team-*&#10;service.name=checkout"></textarea>
</fieldset>
<fieldset><legend>Sample</legend>
  <label><input type="number" id="sample" min="0" max="100" step="1" value="100" style="width:5rem"> % of matching resources</label>
</fieldset>
<p>
  <button id="start">Start</button>
  <button id="stop" disabled>Stop</button>
  <button id="clear">Clear</button>
  <span id="status"></span>
</p>
<div id="out"></div>
<script>
"use strict";
let ctl = null, lines = 0;
const out = document.getElementById("out"), status = document.getElementById("status");
const startBtn = document.getElementById("start"), stopBtn = document.getElementById("stop");
function append(text) {
  out.appendChild(document.createTextNode(text));
  // Bound the DOM: keep roughly the last 500 lines.
  lines += (text.match(/\n/g) || []).length;
  while (lines > 500 && out.firstChild) {
    lines -= (out.firstChild.textContent.match(/\n/g) || []).length;
    out.removeChild(out.firstChild);
  }
  out.scrollTop = out.scrollHeight;
}
async function start() {
  const q = new URLSearchParams();
  for (const s of ["logs", "metrics", "traces"])
    if (document.getElementById("sig-" + s).checked) q.append("signal", s);
  for (const line of document.getElementById("attrs").value.split("\n"))
    if (line.trim()) q.append("attr", line.trim());
  q.set("sample", document.getElementById("sample").value || "100");
  ctl = new AbortController();
  startBtn.disabled = true; stopBtn.disabled = false; status.textContent = "connecting…";
  try {
    const resp = await fetch("/debug/otlp?" + q, {signal: ctl.signal});
    if (!resp.ok) { append(await resp.text()); return; }
    status.textContent = "streaming";
    const reader = resp.body.getReader(), dec = new TextDecoder();
    for (;;) {
      const {done, value} = await reader.read();
      if (done) break;
      append(dec.decode(value, {stream: true}));
    }
  } catch (e) {
    if (e.name !== "AbortError") append("\n# " + e + "\n");
  } finally {
    startBtn.disabled = false; stopBtn.disabled = true; status.textContent = "stopped";
    ctl = null;
  }
}
document.getElementById("start").onclick = start;
document.getElementById("stop").onclick = () => ctl && ctl.abort();
document.getElementById("clear").onclick = () => { out.textContent = ""; lines = 0; };
</script>
`
