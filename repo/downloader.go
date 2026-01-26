package repo

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"path"
)

// HttpDownloader implements the Downloader interface using HTTP.
type HttpDownloader struct {
	fs FileSystem
}

// NewHttpDownloader creates a new HTTP-based downloader.
func NewHttpDownloader(fs FileSystem) Downloader {
	return &HttpDownloader{
		fs: fs,
	}
}

// DownloadFile fetches a URL to a local path with atomic writing and checksum verification.
// It returns the calculated SHA256 on success.
func (h *HttpDownloader) DownloadFile(urlStr string, destPath string, expectedSHA256 string) (string, error) {
	// 1. Ensure the directory structure exists
	if err := h.fs.MkdirAll(path.Dir(destPath), 0o755); err != nil {
		return "", fmt.Errorf("mkdir failed: %v", err)
	}

	// 2. Create a temporary file to avoid corrupting the destination until success
	// We append ".tmp" to the filename
	tmpPath := destPath + ".tmp"
	out, err := h.fs.Create(tmpPath)
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
	if _, err := io.Copy(multiWriter, resp.Body); err != nil {
		return "", fmt.Errorf("copy failed: %v", err)
	}

	// 6. Verify Checksum (if provided)
	calculatedHash := hex.EncodeToString(hasher.Sum(nil))

	if expectedSHA256 != "" && calculatedHash != expectedSHA256 {
		// Clean up the garbage file
		h.fs.Remove(tmpPath)
		return "", fmt.Errorf("checksum mismatch!\nExpected: %s\nActual:   %s", expectedSHA256, calculatedHash)
	}

	// 7. Atomic Rename
	// Close the file explicitly before renaming (defer might be too late)
	out.Close()
	if err := h.fs.Rename(tmpPath, destPath); err != nil {
		return "", fmt.Errorf("rename failed: %v", err)
	}
	return calculatedHash, nil
}
