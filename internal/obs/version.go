package obs

import (
	"runtime/debug"
	"sync"
)

// Version is the build identity, overridable at link time with
// -ldflags="-X github.com/JohanLindvall/kubescrape/internal/obs.Version=v1.2.3".
// Left empty it is derived from the module's embedded VCS stamps, so a plain
// `go build` still reports something truthful.
var Version string

var buildInfoOnce sync.Once
var vcsRevision, vcsTime string
var vcsModified bool

func readBuildInfo() {
	buildInfoOnce.Do(func() {
		info, ok := debug.ReadBuildInfo()
		if !ok {
			return
		}
		for _, s := range info.Settings {
			switch s.Key {
			case "vcs.revision":
				vcsRevision = s.Value
			case "vcs.time":
				vcsTime = s.Value
			case "vcs.modified":
				vcsModified = s.Value == "true"
			}
		}
		if Version == "" && info.Main.Version != "" && info.Main.Version != "(devel)" {
			Version = info.Main.Version
		}
	})
}

// BuildVersion reports the release version, or the VCS revision when the
// binary was not built from a tagged module. "unknown" only when neither is
// available (a build with -buildvcs=false and no -X).
//
// Every binary should be able to answer "which build is this?": without it, a
// panic trace, a metric anomaly or a log line cannot be tied back to a commit,
// and a partially-rolled-out DaemonSet is indistinguishable from a fully
// rolled-out one.
func BuildVersion() string {
	readBuildInfo()
	switch {
	case Version != "":
		return Version
	case vcsRevision != "" && vcsModified:
		return vcsRevision + "-dirty"
	case vcsRevision != "":
		return vcsRevision
	}
	return "unknown"
}

// ScopeVersion is BuildVersion() resolved once, for stamping on the
// instrumentation scope of exported payloads (Scope().SetVersion). Every
// producer already NAMES its scope; none of them versioned it, though the
// version was sitting right here and is already stamped as service.version.
//
// A var rather than a call per site because the scope is named where a
// ScopeLogs/ScopeMetrics is CREATED, and the split and cadvisor batchers create
// one per described object — thousands per scrape on a KSM target.
//
// Wire-visible, and worth knowing before an upgrade: a translation that emits
// scope labels (otel_scope_name / otel_scope_version) will split every stream
// at each release boundary, because that is what a version label is for.
var ScopeVersion = BuildVersion()

// BuildTime reports the VCS commit time, empty when unavailable.
func BuildTime() string {
	readBuildInfo()
	return vcsTime
}
