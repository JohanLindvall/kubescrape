//go:build !race

package kubeconvert

// raceEnabled reports whether this binary was built with -race; see the
// //go:build race sibling of this file.
const raceEnabled = false
