//go:build linux

package cgroupstats

import "syscall"

// fsMagic returns the filesystem magic of the mount holding path. The field is
// int64 on 64-bit Linux and 32-bit elsewhere, hence the widening conversion.
func fsMagic(path string) (int64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, err
	}
	return int64(st.Type), nil
}
