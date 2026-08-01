package journald

// Keep/drop/sample rules and log-derived metrics for journal entries — the
// same cost levers the container-log tailer has.
//
// journald previously had logAttributes and logScrubbing but neither rules nor
// logMetrics, so the only way to sample down a node's kubelet/containerd
// chatter — which dominates a typical journal — was a Starlark transform, the
// escape hatch rather than the ergonomic path. The asymmetry was arbitrary:
// these are the same package-level pieces the tailer uses.
