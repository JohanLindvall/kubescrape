#!/usr/bin/env python3
"""Questions the chaos scenarios ask of the collector's captured self-metrics.

hack/otel-collector.yaml's file exporter writes one OTLP/JSON push per line, and
lib.sh's `grab metrics` concatenates the rotated captures oldest first. This is
the ONE walk over that shape. lib.sh used to carry two copies of it (in
`counters` and `counter_total`), and they had already drifted — a missing
value read as None in one and 0 in the other, and a metric type was tested two
different ways — which is exactly how two readers of one capture come to
disagree about whether a counter moved.

    otlpcap.py print    <capture> [substring]
        the latest value of every kubescrape_* series whose name contains
        substring, one line each (human-readable).
    otlpcap.py total    <capture> <metric>
        the SUM over series of <metric>'s latest values, as a bare integer.
    otlpcap.py snapshot <capture> <metric>...
        a JSON baseline of the named metrics' latest values, per series, plus
        the wall-clock time it was taken. Feed it to `grew`.
    otlpcap.py grew     <baseline> <capture> <metric>...
        exit 1 if any series of the named metrics is HIGHER than in the
        baseline (a series absent from the baseline counts from 0), exit 2 if
        the capture holds no agent self-metric newer than the baseline — the
        comparison would then be of the baseline against itself, which proves
        nothing — and 0 otherwise.

A SERIES is (service.name, instance, metric, data-point attributes), where the
instance is service.instance.id and only falls back to k8s.pod.name. That order
is deliberate: an agent's service.instance.id is its NODE from its first push,
while k8s.pod.name appears only once its self-metadata lookup resolves, so
keying on the pod name split one agent's counter into two series (a stale
pre-resolution one and the live one) and a SUM over them double-counted it.

"Latest" is by the data point's own timeUnixNano (capture order breaks ties),
not by file position alone. A counter that was never incremented is never
exported at all, so an absent series reads as 0.
"""

import json
import sys
import time

AGENT = "kubescrape-agent"


def _attr_value(v):
    # OTLP/JSON AnyValue: exactly one of stringValue/intValue/boolValue/...
    return next(iter(v.values()), "") if v else ""


def _number(dp):
    # asInt is an int64, which protojson renders as a STRING; asDouble is a
    # number. Neither present is a zero the encoder omitted.
    v = dp.get("asInt", dp.get("asDouble", 0))
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def points(path, want):
    """Walk the capture: {series-key: (time, value, display)} for every
    gauge/sum data point of a metric for which want(name) is true, keeping the
    latest per series. Also returns the newest timeUnixNano seen on ANY
    kubescrape_* point from an agent resource (the freshness proof `grew`
    needs)."""
    latest = {}
    agent_newest = 0
    order = 0
    for line in open(path, errors="replace"):
        line = line.strip()
        if not line:
            continue
        try:
            doc = json.loads(line)
        except ValueError:
            continue
        for rm in doc.get("resourceMetrics", []):
            ra = {a["key"]: _attr_value(a.get("value"))
                  for a in rm.get("resource", {}).get("attributes", [])}
            svc = ra.get("service.name", "?")
            inst = ra.get("service.instance.id") or ra.get("k8s.pod.name") or "?"
            display = ra.get("k8s.pod.name") or inst
            for sm in rm.get("scopeMetrics", []):
                for m in sm.get("metrics", []):
                    name = m.get("name", "")
                    if not name.startswith("kubescrape_"):
                        continue
                    for kind in ("gauge", "sum"):
                        for dp in m.get(kind, {}).get("dataPoints", []):
                            ts = int(dp.get("timeUnixNano", 0) or 0)
                            if svc == AGENT and ts > agent_newest:
                                agent_newest = ts
                            if not want(name):
                                continue
                            attrs = ",".join(sorted(
                                f'{a["key"]}={_attr_value(a.get("value"))}'
                                for a in dp.get("attributes", [])))
                            key = json.dumps([svc, inst, name, attrs])
                            order += 1
                            prev = latest.get(key)
                            if prev is None or (ts, order) >= (prev[0], prev[3]):
                                latest[key] = (ts, _number(dp), display, order)
    return {k: (v[0], v[1], v[2]) for k, v in latest.items()}, agent_newest


def _fmt(v):
    return str(int(v)) if v == int(v) else str(v)


def cmd_print(path, sub=""):
    pts, _ = points(path, lambda n: sub in n)
    rows = []
    for key, (_, v, display) in pts.items():
        _, _, name, attrs = json.loads(key)
        rows.append((display, name, attrs, v))
    for display, name, attrs, v in sorted(rows):
        print(f"    {display} {name}{{{attrs}}} = {_fmt(v)}")
    return 0


def cmd_total(path, metric):
    pts, _ = points(path, lambda n: n == metric)
    print(int(sum(v for _, v, _ in pts.values())))
    return 0


def cmd_snapshot(path, *metrics):
    names = set(metrics)
    pts, _ = points(path, lambda n: n in names)
    json.dump({
        "taken_unix_nano": time.time_ns(),
        "metrics": sorted(names),
        "series": {k: v for k, (_, v, _) in pts.items()},
    }, sys.stdout)
    print()
    return 0


def cmd_grew(baseline_path, path, *metrics):
    base = json.load(open(baseline_path))
    names = set(metrics)
    missing = names - set(base.get("metrics", []))
    if missing:
        print(f"    the baseline was not taken for {sorted(missing)}", file=sys.stderr)
        return 3
    pts, agent_newest = points(path, lambda n: n in names)
    if agent_newest <= base["taken_unix_nano"]:
        print("    the capture holds no agent self-metric pushed after the baseline "
              "was taken, so a flat loss counter here would prove nothing "
              "(is the agents' -self-metrics-interval push reaching the collector?)",
              file=sys.stderr)
        return 2
    grew = False
    for key in sorted(pts, key=lambda k: json.loads(k)):
        _, after, display = pts[key]
        before = base["series"].get(key, 0.0)
        _, _, name, attrs = json.loads(key)
        moved = after > before
        grew = grew or moved
        print(f"    {display} {name}{{{attrs}}}: {_fmt(before)} -> {_fmt(after)}"
              + ("   <-- MOVED" if moved else ""))
    if not pts:
        print(f"    none of {sorted(names)} has ever been incremented on any agent")
    return 1 if grew else 0


def main(argv):
    cmds = {"print": (cmd_print, 1, 2), "total": (cmd_total, 2, 2),
            "snapshot": (cmd_snapshot, 2, None), "grew": (cmd_grew, 3, None)}
    if len(argv) < 2 or argv[1] not in cmds:
        print(__doc__, file=sys.stderr)
        return 3
    fn, lo, hi = cmds[argv[1]]
    args = argv[2:]
    if len(args) < lo or (hi is not None and len(args) > hi):
        print(__doc__, file=sys.stderr)
        return 3
    return fn(*args)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
