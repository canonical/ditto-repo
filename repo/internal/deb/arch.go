package deb

import "strings"

// ArchOf returns the architecture a repository-relative path belongs to in archs,
// or "" if it is not architecture-specific. It matches against binary index directories
// ("binary-<arch>/"), command-not-found files ("Commands-<arch>."), DEP-11 metadata
// ("Components-<arch>."), and package filenames ("_<arch>/deb"). Matching is delimited
// so that, for example, "amd64" does not match an "amd64v3" path.
func ArchOf(path string, archs []string) string {
	for _, arch := range archs {
		if arch == "" {
			continue
		}

		if containsOne(
			path,
			"binary-"+arch+"/",
			"Commands-"+arch+".",
			"Components-"+arch+".",
		) || strings.HasSuffix(path, "_"+arch+".deb") {
			return arch
		}
	}
	return ""
}

// containsOne returns true if s contains one of substrs.
func containsOne(s string, substrs ...string) bool {
	for _, substr := range substrs {
		if strings.Contains(s, substr) {
			return true
		}
	}
	return false
}
