package deb

// CandidateURLs returns the ordered list of mirror base URLs to try for a given
// repository-relative path. When an architecture-specific preference applies, that base
// is placed first. The remaining URLs follow in their configured order. Duplicates are
// removed while preserving order.
func CandidateURLs(bases []string, preferred string) []string {
	ordered := make([]string, 0, len(bases)+1)
	if preferred != "" {
		ordered = append(ordered, preferred)
	}
	ordered = append(ordered, bases...)

	seen := make(map[string]bool, len(ordered))
	deduped := ordered[:0]
	for _, base := range ordered {
		if base == "" || seen[base] {
			continue
		}
		seen[base] = true
		deduped = append(deduped, base)
	}
	return deduped
}
