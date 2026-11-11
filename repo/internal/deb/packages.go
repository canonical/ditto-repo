package deb

import (
	"bufio"
	"io"
	"strconv"
	"strings"
)

// Debian repository Package index stanza parsing functions.
// See https://wiki.debian.org/DebianRepository/Format#A.22Packages.22_Indices
// for their format.

// PackageMeta holds the download path and integrity data for a single .deb.
type PackageMeta struct {
	Path   string
	SHA256 string
	Size   int64
}

// ParsePackagesIndex scans reader, extracting a list of package metadata
// objects with filenames and checksums.
func ParsePackagesIndex(reader io.Reader) ([]PackageMeta, error) {
	var packages []PackageMeta
	scanner := bufio.NewScanner(reader)

	// Increase buffer size to handle very long lines
	// (the Debian Description fields can be huge).
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 5*1024*1024)

	// State variables for the current block
	var currentPkg PackageMeta
	inBlock := false

	for scanner.Scan() {
		line := scanner.Text()

		// A blank line indicates the end of a package stanza.
		if strings.TrimSpace(line) == "" {
			if inBlock && currentPkg.Path != "" && currentPkg.SHA256 != "" {
				packages = append(packages, currentPkg)
			}
			// Reset for next block.
			currentPkg = PackageMeta{}
			inBlock = false
			continue
		}

		inBlock = true

		// Simple prefix parsing.
		// A robust parser usually handles multiline values (where lines start with spaces),
		// but Filename and SHA256 are always single lines in standard Debian repos.
		if path, pathOk := strings.CutPrefix(line, "Filename: "); pathOk {
			currentPkg.Path = path
		} else if sha256, sha256Ok := strings.CutPrefix(line, "SHA256: "); sha256Ok {
			currentPkg.SHA256 = sha256
		} else if sizeStr, sizeOk := strings.CutPrefix(line, "Size: "); sizeOk {
			if size, err := strconv.ParseInt(sizeStr, 10, 64); err == nil {
				currentPkg.Size = size
			}
		}
	}

	// If the file doesn't end in a newline, we need to handle the last block.
	if inBlock && currentPkg.Path != "" && currentPkg.SHA256 != "" {
		packages = append(packages, currentPkg)
	}

	return packages, scanner.Err()
}
