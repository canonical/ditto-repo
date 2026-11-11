package deb

import (
	"bufio"
	"strings"
)

// Pure functions for handling Debian repository Release files.
// See https://wiki.debian.org/DebianRepository/Format#A.22Release.22_files for their format.

// ParseRelease extracts paths to desired index files that pass the provided filter.
// These only include compressed files - gz, xz, and bz2.
func ParseRelease(contents string, desired func(string) bool) []string {
	var relevantFiles []string
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

			// Filter: check if this file belongs to our desired Components/Archs
			// Path looks like: main/binary-amd64/*.(gz|xz|bz2)
			if desired(filePath) {
				relevantFiles = append(relevantFiles, filePath)
			}
		}
	}
	return relevantFiles
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
