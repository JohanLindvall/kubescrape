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

// BuildTime reports the VCS commit time, empty when unavailable.
func BuildTime() string {
	readBuildInfo()
	return vcsTime
}
