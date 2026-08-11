//go:build !linux

package cgroupstats

import "errors"

// fsMagic has no meaning off Linux. checkCgroup2 falls back to the
// cgroup.controllers probe, which is what makes the package's tests runnable
// anywhere even though cgroups themselves are Linux-only. The pair exists so a
// `go build ./...` on a developer's non-Linux machine does not fail on a
// syscall.Statfs_t field that only Linux has.
func fsMagic(string) (int64, error) { return 0, errors.ErrUnsupported }
