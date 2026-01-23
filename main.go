package main

import (
	"bufio"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// --- Configuration ---
var (
	RepoURL      = "http://ppa.launchpadcontent.net/mitchburton/snap-http/ubuntu"
	Dist         = "noble"
	Components   = []string{"main"}
	Archs        = []string{"amd64"}
	Languages    = []string{"en"} // Add languages here (e.g. "en", "es")
	DownloadPath = "./mirror"     // Local storage root
)

// PackageMeta holds the download path and integrity data for a single .deb
type PackageMeta struct {
	Path   string
	SHA256 string
}

func main() {
	fmt.Printf("Starting mirror of %s [%s]...\n", RepoURL, Dist)

	// 1. Fetch Repository Metadata (Signatures & Release file)
	// We must fetch these byte-for-byte to preserve upstream signatures.
	metadataFiles := []string{"InRelease", "Release", "Release.gpg"}
	for _, meta := range metadataFiles {
		url := fmt.Sprintf("%s/dists/%s/%s", RepoURL, Dist, meta)
		dest := path.Join(DownloadPath, "dists", Dist, meta)

		fmt.Printf("Fetching Metadata: %s... ", meta)
		// We pass "" as checksum because we don't know it yet (it's the source of truth)
		if _, err := downloadFile(url, dest, ""); err != nil {
			// InRelease is optional if Release.gpg exists, but usually good to have.
			// Release and Release.gpg are critical.
			fmt.Printf("Warning/Error: %v\n", err)
		} else {
			fmt.Println("OK")
		}
	}

	// 2. Read the local 'Release' file to parse package indices
	// We read from disk instead of fetching again to ensure consistency.
	releasePath := path.Join(DownloadPath, "dists", Dist, "Release")
	releaseBytes, err := os.ReadFile(releasePath)
	if err != nil {
		panic(fmt.Errorf("could not read local Release file: %v", err))
	}

	packageIndices := parseReleaseFile(string(releaseBytes))

	// 3. Process each Package Index (Packages & Translations)
	for _, idxPath := range packageIndices {
		fmt.Printf("Processing Index: %s\n", idxPath)

		fullIndexURL := fmt.Sprintf("%s/dists/%s/%s", RepoURL, Dist, idxPath)
		localIndexPath := path.Join(DownloadPath, "dists", Dist, idxPath)

		// Download the Index (Packages.gz) itself
		// Note: Ideally, we should verify the SHA256 of this index file against the Release file here.
		// For this prototype, we just download it.
		calculatedHash, err := downloadFile(fullIndexURL, localIndexPath, "")
		if err != nil {
			fmt.Printf("  Failed to download index: %v\n", err)
			continue
		}

		// We have the file and its hash. Create the alias so modern clients are happy.
		if err := createByHashLink(localIndexPath, calculatedHash); err != nil {
			fmt.Printf("  Warning: failed to create by-hash link: %v\n", err)
		}

		// Only looks for .debs inside "Packages" files, not "Translation" files
		if strings.Contains(idxPath, "Packages") {
			processPackageIndex(localIndexPath)
		}
	}

	fmt.Println("\nMirror complete.")
}

func processPackageIndex(localIndexPath string) {
	debs, err := extractDebsFromIndex(localIndexPath)
	if err != nil {
		fmt.Printf("  Error parsing index: %v\n", err)
		return
	}

	fmt.Printf("  -> Found %d packages. Checking pool...\n", len(debs))

	// 4. Download the actual .deb files
	for _, pkg := range debs {
		absURL := fmt.Sprintf("%s/%s", RepoURL, pkg.Path)
		localPath := path.Join(DownloadPath, pkg.Path)

		// Check if file already exists
		if _, err := os.Stat(localPath); err == nil {
			// File exists, verify checksum
			fmt.Printf("Verifying existing: %s... ", pkg.Path)
			match, _ := verifyFile(localPath, pkg.SHA256)
			if match {
				fmt.Println("OK (Skipping download)")
				continue
			}
			fmt.Println("Mismatch (Redownloading)")
		}

		// Download with validation
		fmt.Printf("     Downloading %s... ", path.Base(pkg.Path))
		if _, err := downloadFile(absURL, localPath, pkg.SHA256); err != nil {
			fmt.Printf("FAILED: %v\n", err)
			// We may want to retry or exit here
		} else {
			fmt.Println("OK")
		}
	}
}

// parseReleaseFile extracts paths to Packages.gz that match our Arch/Component filter
// Also suports Translation files (bz2, usually)
func parseReleaseFile(content string) []string {
	var relevantFiles []string
	scanner := bufio.NewScanner(strings.NewReader(content))
	inSha256Block := false

	for scanner.Scan() {
		line := scanner.Text()

		// The Release file format has "SHA256:" followed by indented lines of files
		if strings.HasPrefix(line, "SHA256:") {
			inSha256Block = true
			continue
		}
		// If we hit another key (no indentation), we exited the block
		if inSha256Block && len(line) > 0 && line[0] != ' ' {
			inSha256Block = false
		}

		if inSha256Block {
			parts := strings.Fields(line)
			if len(parts) < 3 {
				continue
			}
			filePath := parts[2] // Format: checksum size filename

			validExt := strings.HasSuffix(filePath, ".gz") ||
				strings.HasSuffix(filePath, ".xz") ||
				strings.HasSuffix(filePath, ".bz2")

			// Filter: We only want "Packages.gz" or "Packages.xz"
			if !validExt {
				continue
			}

			// Filter: Check if this file belongs to our desired Components/Archs
			// Path looks like: main/binary-amd64/Packages.gz
			if isDesired(filePath) {
				relevantFiles = append(relevantFiles, filePath)
			}
		}
	}
	return relevantFiles
}

// isDesired checks if a file path string matches our Component/Arch config
func isDesired(filePath string) bool {
	// Check Component
	matchedComponent := false
	for _, c := range Components {
		if strings.HasPrefix(filePath, c+"/") {
			matchedComponent = true
			break
		}
	}
	if !matchedComponent {
		return false
	}

	// Check Type: Architecture Binary OR Translation
	isBinary := false
	for _, a := range Archs {
		if strings.Contains(filePath, "binary-"+a+"/") && strings.Contains(filePath, "Packages") {
			isBinary = true
			break
		}
	}

	isTranslation := false
	if strings.Contains(filePath, "i18n/Translation-") {
		for _, lang := range Languages {
			// Matches Translation-en.gz, Translation-en_GB.bz2, etc.
			if strings.Contains(filePath, "Translation-"+lang) {
				isTranslation = true
				break
			}
		}
	}

	return isBinary || isTranslation
}

// extractDebsFromIndex parses a local Packages.gz file
// returning a list of PackageMeta objects with filenames and checksums.
func extractDebsFromIndex(localPath string) ([]PackageMeta, error) {
	f, err := os.Open(localPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Handle GZIP automatically
	var reader io.Reader = f
	if strings.HasSuffix(localPath, ".gz") {
		gzReader, err := gzip.NewReader(f)
		if err != nil {
			return nil, err
		}
		defer gzReader.Close()
		reader = gzReader
	} else if strings.HasSuffix(localPath, ".xz") {
		// Note: Standard Go library doesn't support XZ.
		// We would need "github.com/ulikunitz/xz" or simply avoid .xz indices if possible.
		return nil, fmt.Errorf("xz compression not implemented in this snippet")
	}

	var packages []PackageMeta
	scanner := bufio.NewScanner(reader)

	// Increase buffer size to handle ver long lines (Debian Description fields can be huge)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 5*1024*1024)

	// State variables for the current block
	var currentPkg PackageMeta
	inBlock := false

	for scanner.Scan() {
		line := scanner.Text()

		// A blank line indicates the end of a package stanza
		if strings.TrimSpace(line) == "" {
			if inBlock && currentPkg.Path != "" && currentPkg.SHA256 != "" {
				packages = append(packages, currentPkg)
			}
			// Reset for next block
			currentPkg = PackageMeta{}
			inBlock = false
			continue
		}

		inBlock = true

		// Simple prefix parsing.
		// Note: A robust parser usually handles multiline values (lines starting with space).
		// but Filename and SHA256 are always single lines in standard Debian repos.
		if strings.HasPrefix(line, "Filename: ") {
			currentPkg.Path = strings.TrimPrefix(line, "Filename: ")
		} else if strings.HasPrefix(line, "SHA256: ") {
			currentPkg.SHA256 = strings.TrimPrefix(line, "SHA256: ")
		}
	}

	// Handle the very last block if the file doesn't end with a newline
	if inBlock && currentPkg.Path != "" && currentPkg.SHA256 != "" {
		packages = append(packages, currentPkg)
	}

	return packages, scanner.Err()
}

// verifyFile is a helper function to check a downloaded file against the expected checksum
func verifyFile(filepath string, expectedSHA256 string) (bool, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return false, err
	}
	defer f.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, f); err != nil {
		return false, err
	}

	calculated := hex.EncodeToString(hasher.Sum(nil))
	return calculated == expectedSHA256, nil
}

// downloadFile fetches a URL to a local path with atomic writing and checksum verification.
// It returns the calculated SHA256 on success.
func downloadFile(urlStr string, destPath string, expectedSHA256 string) (string, error) {
	// 1. Ensure the directory structure exists
	if err := os.MkdirAll(path.Dir(destPath), 0o755); err != nil {
		return "", fmt.Errorf("mkdir failed: %v", err)
	}

	// 2. Create a temporary file to avoid corrupting the destination until success
	// We append ".tmp" to the filename
	tmpPath := destPath + ".tmp"
	out, err := os.Create(tmpPath)
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %v", err)
	}
	defer out.Close()

	// 3. Perform the HTTP Request
	resp, err := http.Get(urlStr)
	if err != nil {
		return "", fmt.Errorf("http error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("status %d", resp.StatusCode)
	}

	// 4. Set up hashing while downloading (Streaming)
	// We write to both the file ('out') and the sha256 calculator ('hasher') simultaneously.
	hasher := sha256.New()
	multiWriter := io.MultiWriter(out, hasher)

	// 5. Copy the data
	copiedBytes, err := io.Copy(multiWriter, resp.Body)
	if err != nil {
		return "", fmt.Errorf("copy failed: %v", err)
	}

	// 6. Verify Checksum (if provided)
	calculatedHash := hex.EncodeToString(hasher.Sum(nil))

	if expectedSHA256 != "" && calculatedHash != expectedSHA256 {
		// Clean up the garbage file
		os.Remove(tmpPath)
		return "", fmt.Errorf("checksum mismatch!\nExpected: %s\nActual:   %s", expectedSHA256, calculatedHash)
	}

	// 7. Atomic Rename
	// Close the file explicitly before renaming (defer might be too late)
	out.Close()
	if err := os.Rename(tmpPath, destPath); err != nil {
		return "", fmt.Errorf("rename failed: %v", err)
	}

	fmt.Printf("Downloaded: %s (%d bytes)\n", path.Base(destPath), copiedBytes)
	return calculatedHash, nil
}

// createByHashLink creates a hardlink (or copy) in the by-hash/SHA256/ directory
func createByHashLink(originalPath string, hash string) error {
	// originalPath: .../main/binary-amd64/Packages.gz
	// targetDir:    .../main/binary-amd64/by-hash/SHA256
	dir := filepath.Dir(originalPath)
	byHashDir := filepath.Join(dir, "by-hash", "SHA256")

	if err := os.MkdirAll(byHashDir, 0o755); err != nil {
		return err
	}

	targetPath := filepath.Join(byHashDir, hash)

	// Remove existing if present to ensure freshness
	os.Remove(targetPath)

	// Try Hardlink first (fastest, saves space)
	err := os.Link(originalPath, targetPath)
	if err != nil {
		// Fallback to Copy if hardlink fails (e.g. cross-device)
		src, err := os.Open(originalPath)
		if err != nil {
			return err
		}
		defer src.Close()
		dst, err := os.Create(targetPath)
		if err != nil {
			return err
		}
		defer dst.Close()
		_, err = io.Copy(dst, src)
		return err
	}
	return nil
}
