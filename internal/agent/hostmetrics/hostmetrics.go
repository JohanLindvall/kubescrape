// Package hostmetrics collects node-level system metrics straight from
// /proc (github.com/prometheus/procfs — node_exporter's own parser) and
// exports them as OTLP, eliminating the separate node_exporter DaemonSet for
// the core metric set. Metric NAMES are node_exporter-compatible
// (node_cpu_seconds_total, node_memory_*_bytes, ...) so existing Grafana
// dashboards and alerts keep working.
//
// In-cluster the agent reads the HOST's /proc (mount it and point -host-proc
// at it); filesystem usage additionally needs the host root mounted
// (-host-rootfs) for statfs and is skipped when unset.
package hostmetrics

import (
	"context"
	"log/slog"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/procfs"
	"github.com/prometheus/procfs/blockdevice"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"golang.org/x/sys/unix"
)

// Exporter ships one metrics payload.
type Exporter interface {
	ExportMetrics(ctx context.Context, md pmetric.Metrics) error
}

// Config configures the collector.
type Config struct {
	// ProcPath is the proc filesystem to read (host /proc mounted into the
	// container, e.g. /host/proc; plain /proc outside Kubernetes).
	ProcPath string
	// RootfsPath, when set, prefixes mountpoints for filesystem usage
	// (statfs); empty skips filesystem metrics.
	RootfsPath string
	// Interval between collections (default 30s).
	Interval time.Duration
	// Node stamps k8s.node.name on the resource.
	Node string

	Exporter Exporter
	Logger   *slog.Logger
}

// Collector reads /proc and exports node metrics.
type Collector struct {
	cfg   Config
	fs    procfs.FS
	block blockdevice.FS
	log   *slog.Logger
	res   pcommon.Resource
}

// New creates a Collector (fails fast when ProcPath is unreadable).
func New(cfg Config) (*Collector, error) {
	if cfg.ProcPath == "" {
		cfg.ProcPath = "/proc"
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	fs, err := procfs.NewFS(cfg.ProcPath)
	if err != nil {
		return nil, err
	}
	// The sys mount is only needed for per-device sysfs stats we don't read;
	// ProcDiskstats parses <proc>/diskstats.
	block, err := blockdevice.NewFS(cfg.ProcPath, "/sys")
	if err != nil {
		return nil, err
	}
	res := pcommon.NewResource()
	a := res.Attributes()
	// job="node" / instance=<node name> after the Mimir mapping — what
	// node_exporter dashboards select on.
	a.PutStr("service.name", "node")
	if cfg.Node != "" {
		a.PutStr("k8s.node.name", cfg.Node)
		a.PutStr("service.instance.id", cfg.Node)
	}
	return &Collector{cfg: cfg, fs: fs, block: block, log: cfg.Logger, res: res}, nil
}

// Run collects on the interval until ctx ends.
func (c *Collector) Run(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			md := c.collect()
			exportCtx, cancel := context.WithTimeout(ctx, c.cfg.Interval)
			if err := c.cfg.Exporter.ExportMetrics(exportCtx, md); err != nil && ctx.Err() == nil {
				c.log.Warn("exporting host metrics", "error", err)
			}
			cancel()
		}
	}
}

// collect gathers one snapshot.
func (c *Collector) collect() pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	c.res.CopyTo(rm.Resource())
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("github.com/JohanLindvall/kubescrape/agent/hostmetrics")
	now := pcommon.NewTimestampFromTime(time.Now())
	b := builder{ms: sm.Metrics(), now: now}

	c.cpu(&b)
	c.memory(&b)
	c.load(&b)
	c.disk(&b)
	c.network(&b)
	c.filesystems(&b)
	return md
}

// builder appends node_exporter-shaped series.
type builder struct {
	ms  pmetric.MetricSlice
	now pcommon.Timestamp
}

// counter appends a cumulative monotonic sum data point.
func (b *builder) counter(name, unit string, v float64, attrs ...string) {
	b.point(name, unit, v, true, attrs...)
}

// gauge appends a gauge data point.
func (b *builder) gauge(name, unit string, v float64, attrs ...string) {
	b.point(name, unit, v, false, attrs...)
}

func (b *builder) point(name, unit string, v float64, counter bool, attrs ...string) {
	// One Metric per (name) with appended points would need bookkeeping;
	// series here are few hundred, so find-or-append linearly per call is
	// fine at a 30s cadence.
	var m pmetric.Metric
	found := false
	for i := 0; i < b.ms.Len(); i++ {
		if b.ms.At(i).Name() == name {
			m = b.ms.At(i)
			found = true
			break
		}
	}
	if !found {
		m = b.ms.AppendEmpty()
		m.SetName(name)
		m.SetUnit(unit)
		if counter {
			s := m.SetEmptySum()
			s.SetIsMonotonic(true)
			s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		} else {
			m.SetEmptyGauge()
		}
	}
	var dp pmetric.NumberDataPoint
	if counter {
		dp = m.Sum().DataPoints().AppendEmpty()
	} else {
		dp = m.Gauge().DataPoints().AppendEmpty()
	}
	dp.SetTimestamp(b.now)
	dp.SetDoubleValue(v)
	for i := 0; i+1 < len(attrs); i += 2 {
		dp.Attributes().PutStr(attrs[i], attrs[i+1])
	}
}

func (c *Collector) cpu(b *builder) {
	stat, err := c.fs.Stat()
	if err != nil {
		c.log.Warn("reading /proc/stat", "error", err)
		return
	}
	for i, cpu := range stat.CPU {
		id := strconv.FormatInt(i, 10)
		b.counter("node_cpu_seconds_total", "s", cpu.User, "cpu", id, "mode", "user")
		b.counter("node_cpu_seconds_total", "s", cpu.System, "cpu", id, "mode", "system")
		b.counter("node_cpu_seconds_total", "s", cpu.Idle, "cpu", id, "mode", "idle")
		b.counter("node_cpu_seconds_total", "s", cpu.Iowait, "cpu", id, "mode", "iowait")
		b.counter("node_cpu_seconds_total", "s", cpu.Nice, "cpu", id, "mode", "nice")
		b.counter("node_cpu_seconds_total", "s", cpu.IRQ, "cpu", id, "mode", "irq")
		b.counter("node_cpu_seconds_total", "s", cpu.SoftIRQ, "cpu", id, "mode", "softirq")
		b.counter("node_cpu_seconds_total", "s", cpu.Steal, "cpu", id, "mode", "steal")
	}
	b.gauge("node_boot_time_seconds", "s", float64(stat.BootTime))
	b.counter("node_context_switches_total", "1", float64(stat.ContextSwitches))
	b.counter("node_forks_total", "1", float64(stat.ProcessCreated))
}

func (c *Collector) memory(b *builder) {
	mi, err := c.fs.Meminfo()
	if err != nil {
		c.log.Warn("reading /proc/meminfo", "error", err)
		return
	}
	kb := func(v *uint64) float64 {
		if v == nil {
			return 0
		}
		return float64(*v) * 1024
	}
	b.gauge("node_memory_MemTotal_bytes", "By", kb(mi.MemTotal))
	b.gauge("node_memory_MemFree_bytes", "By", kb(mi.MemFree))
	b.gauge("node_memory_MemAvailable_bytes", "By", kb(mi.MemAvailable))
	b.gauge("node_memory_Buffers_bytes", "By", kb(mi.Buffers))
	b.gauge("node_memory_Cached_bytes", "By", kb(mi.Cached))
	b.gauge("node_memory_SwapTotal_bytes", "By", kb(mi.SwapTotal))
	b.gauge("node_memory_SwapFree_bytes", "By", kb(mi.SwapFree))
}

func (c *Collector) load(b *builder) {
	la, err := c.fs.LoadAvg()
	if err != nil {
		c.log.Warn("reading /proc/loadavg", "error", err)
		return
	}
	b.gauge("node_load1", "1", la.Load1)
	b.gauge("node_load5", "1", la.Load5)
	b.gauge("node_load15", "1", la.Load15)
}

func (c *Collector) disk(b *builder) {
	stats, err := c.block.ProcDiskstats()
	if err != nil {
		c.log.Warn("reading /proc/diskstats", "error", err)
		return
	}
	for _, d := range stats {
		// Skip partitions and virtual devices the dashboards ignore; the
		// heuristic node_exporter uses is a device-name pattern — keep it
		// simple: skip loop/ram devices.
		if strings.HasPrefix(d.DeviceName, "loop") || strings.HasPrefix(d.DeviceName, "ram") {
			continue
		}
		b.counter("node_disk_reads_completed_total", "1", float64(d.ReadIOs), "device", d.DeviceName)
		b.counter("node_disk_writes_completed_total", "1", float64(d.WriteIOs), "device", d.DeviceName)
		b.counter("node_disk_read_bytes_total", "By", float64(d.ReadSectors)*512, "device", d.DeviceName)
		b.counter("node_disk_written_bytes_total", "By", float64(d.WriteSectors)*512, "device", d.DeviceName)
		b.counter("node_disk_io_time_seconds_total", "s", float64(d.IOsTotalTicks)/1000, "device", d.DeviceName)
	}
}

func (c *Collector) network(b *builder) {
	nd, err := c.fs.NetDev()
	if err != nil {
		c.log.Warn("reading /proc/net/dev", "error", err)
		return
	}
	for _, d := range nd {
		if d.Name == "lo" {
			continue
		}
		b.counter("node_network_receive_bytes_total", "By", float64(d.RxBytes), "device", d.Name)
		b.counter("node_network_transmit_bytes_total", "By", float64(d.TxBytes), "device", d.Name)
		b.counter("node_network_receive_packets_total", "1", float64(d.RxPackets), "device", d.Name)
		b.counter("node_network_transmit_packets_total", "1", float64(d.TxPackets), "device", d.Name)
		b.counter("node_network_receive_errs_total", "1", float64(d.RxErrors), "device", d.Name)
		b.counter("node_network_transmit_errs_total", "1", float64(d.TxErrors), "device", d.Name)
	}
}

// virtualFS are filesystem types never worth reporting.
var virtualFS = map[string]bool{
	"proc": true, "sysfs": true, "cgroup": true, "cgroup2": true, "tmpfs": true,
	"devtmpfs": true, "devpts": true, "overlay": true, "squashfs": true,
	"tracefs": true, "debugfs": true, "securityfs": true, "fusectl": true,
	"configfs": true, "bpf": true, "pstore": true, "mqueue": true, "hugetlbfs": true,
	"rpc_pipefs": true, "nsfs": true, "autofs": true, "binfmt_misc": true,
}

func (c *Collector) filesystems(b *builder) {
	if c.cfg.RootfsPath == "" {
		return // statfs needs the host root mounted; skipped when absent
	}
	mounts, err := c.fs.GetMounts()
	if err != nil {
		c.log.Warn("reading mountinfo", "error", err)
		return
	}
	seen := map[string]bool{}
	for _, m := range mounts {
		if m == nil || virtualFS[m.FSType] || seen[m.MountPoint] {
			continue
		}
		seen[m.MountPoint] = true
		var st unix.Statfs_t
		if err := unix.Statfs(filepath.Join(c.cfg.RootfsPath, m.MountPoint), &st); err != nil {
			continue
		}
		bs := float64(st.Bsize)
		attrs := []string{"device", m.Source, "mountpoint", m.MountPoint, "fstype", m.FSType}
		b.gauge("node_filesystem_size_bytes", "By", float64(st.Blocks)*bs, attrs...)
		b.gauge("node_filesystem_avail_bytes", "By", float64(st.Bavail)*bs, attrs...)
		b.gauge("node_filesystem_free_bytes", "By", float64(st.Bfree)*bs, attrs...)
	}
}
