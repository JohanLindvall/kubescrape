//go:build !linux

package cgroupstats

import "os"

// dirBufBytes is unused off Linux; it is declared so the scan's scratch buffer
// has one size on every platform.
const dirBufBytes = 4096

// subdirs is the portable form: os.ReadDir, filtered. It allocates a DirEntry
// per control file (see readdir_linux.go for why that matters), which is
// acceptable here because there is no cgroup v2 hierarchy to walk off Linux —
// this file exists so the package still builds and its parsing tests still run
// on a developer's machine.
func subdirs(dir string, _ []byte) ([]string, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range ents {
		if e.IsDir() {
			out = append(out, e.Name())
		}
	}
	return out, nil
}
