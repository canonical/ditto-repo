package deb

import (
	"bufio"
	"strconv"
	"strings"
)

// Pure functions for handling Debian repository Release files.
// See https://wiki.debian.org/DebianRepository/Format#A.22Release.22_files for their format.

// The mirror's desired subset of a distribution: which components,
// architectures, and translation languages to pull indices for.
type SelectionFilter struct {
	Components []string
	Archs      []string
	Languages  []string
}

// Matches returns true if path is selected for by the filter settings.
func (f SelectionFilter) Matches(path string) bool {
	// Ignore anything not in a listed component.
	matchedComponent := false
	for _, c := range f.Components {
		if strings.HasPrefix(path, c+"/") {
			matchedComponent = true
			break
		}
	}
	if !matchedComponent {
		return false
	}

	// Check for matching-architecture binary Package index.
	for _, a := range f.Archs {
		if strings.Contains(path, "binary-"+a+"/") && strings.Contains(path, "Packages") {
			return true
		}
	}

	// Check for matching-language translation file.
	for _, lang := range f.Languages {
		// Matches Translation-en.gz, Translation-en_GB.bz2, etc.
		if strings.Contains(path, "Translation-"+lang) {
			return true
		}
	}

	// Check for CNF (command-not-found) files.
	if strings.Contains(path, "cnf/Commands-") {
		for _, a := range f.Archs {
			// Matches Commands-amd64.xz, Commands-arm64.xz, etc.
			if strings.Contains(path, "Commands-"+a) {
				return true
			}
		}
	}

	// Check for DEP-11 (Appstream metadata) files. These live under <component>/dep11/
	// and are listed directly in the Release file, so they only need copying.
	if strings.Contains(path, "/dep11") {
		if strings.Contains(path, "/dep11/Components-") {
			// Components-<arch>.yml.* files are architecture-specific.
			for _, a := range f.Archs {
				if strings.Contains(path, "Components-"+a+".") {
					return true
				}
			}
		} else {
			// Other dep11 files (e.g. icons-<size>.tar.gz) are not arch-specific.
			return true
		}
	}

	// No matches in any above cases.
	return false
}

// A single index file entry in the Release file.
type ReleaseEntry struct {
	SHA256 string
	Size   int64
	Path   string // e.g., "main/binary-amd64/Packages.gz"
}

// ParseRelease extracts paths to desired index files that pass the provided filter.
// These only include compressed files - gz, xz, and bz2.
func ParseRelease(contents string) []ReleaseEntry {
	entries := make([]ReleaseEntry, 0)
	scanner := bufio.NewScanner(strings.NewReader(contents))
	inSha256Block := false

	for scanner.Scan() {
		line := scanner.Text()

		// The Release file format has "SHA256:" followed by indented lines of files.
		if strings.HasPrefix(line, "SHA256:") {
			inSha256Block = true
			continue
		}
		// If we hit another key (no indentation), we have exited the block.
		if inSha256Block && len(line) > 0 && line[0] != ' ' {
			inSha256Block = false
		}

		if inSha256Block {
			parts := strings.Fields(line)
			if len(parts) < 3 {
				continue
			}
			filePath := parts[2] // Format: checksum size filename

			if !hasOneOfSuffix(filePath, ".gz", ".xz", ".bz2") {
				continue
			}

			entry := ReleaseEntry{SHA256: parts[0], Path: parts[2]}
			if size, err := strconv.ParseInt(parts[1], 10, 64); err == nil {
				entry.Size = size
			}

			entries = append(entries, entry)
		}
	}
	return entries
}

// SelectIndices filters entries to contain only those that pass the filter.
func SelectIndices(entries []ReleaseEntry, f SelectionFilter) []ReleaseEntry {
	filtered := make([]ReleaseEntry, 0)
	for _, entry := range entries {
		if f.Matches(entry.Path) {
			filtered = append(filtered, entry)
		}
	}
	return filtered
}

// hasOneSuffix returns true if s ends with any of suffixes.
func hasOneOfSuffix(s string, suffixes ...string) bool {
	for _, suf := range suffixes {
		if strings.HasSuffix(s, suf) {
			return true
		}
	}
	return false
}
