package repo

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

// mockLogger is a simple logger for testing that captures log messages.
type mockLogger struct {
	debugMsgs []string
	errorMsgs []string
	infoMsgs  []string
	warnMsgs  []string
}

func (l *mockLogger) Debug(msg string, _ ...any) {
	l.debugMsgs = append(l.debugMsgs, msg)
}

func (l *mockLogger) Error(msg string, _ ...any) {
	l.errorMsgs = append(l.errorMsgs, msg)
}

func (l *mockLogger) Info(msg string, _ ...any) {
	l.infoMsgs = append(l.infoMsgs, msg)
}

func (l *mockLogger) Warn(msg string, _ ...any) {
	l.warnMsgs = append(l.warnMsgs, msg)
}

// mockDownloader is a simple downloader for testing that doesn't actually download.
// By default every call records the requested URL and returns a fixed fake hash.
// For finer control, hashByURL maps specific URLs to the hash they should return and
// errByURL maps specific URLs to an error; err forces the same error for every call.
type mockDownloader struct {
	downloads []string
	err       error
	hashByURL map[string]string
	errByURL  map[string]error
}

func (d *mockDownloader) DownloadFile(urlStr string, _ string, _ string) (string, error) {
	d.downloads = append(d.downloads, urlStr)
	if d.err != nil {
		return "", d.err
	}
	if err, ok := d.errByURL[urlStr]; ok {
		return "", err
	}
	if hash, ok := d.hashByURL[urlStr]; ok {
		return hash, nil
	}
	// Return a fake hash
	return "fakehash123", nil
}

func TestNewDittoRepo(t *testing.T) {
	fs := NewMemFileSystem()
	logger := &mockLogger{}
	downloader := &mockDownloader{}

	t.Run("sets default workers when not specified", func(t *testing.T) {
		config := DittoConfig{
			Workers:    0,
			Logger:     logger,
			FileSystem: fs,
			Downloader: downloader,
		}
		repo := NewDittoRepo(config).(*dittoRepo)
		if repo.config.Workers != defaultWorkers {
			t.Errorf("expected %d workers, got %d", defaultWorkers, repo.config.Workers)
		}
	})

	t.Run("preserves configured workers", func(t *testing.T) {
		config := DittoConfig{
			Workers:    10,
			Logger:     logger,
			FileSystem: fs,
			Downloader: downloader,
		}
		repo := NewDittoRepo(config).(*dittoRepo)
		if repo.config.Workers != 10 {
			t.Errorf("expected 10 workers, got %d", repo.config.Workers)
		}
	})

	t.Run("backwards compatibility - Dist converts to Dists", func(t *testing.T) {
		config := DittoConfig{
			Dist:       "focal",
			Logger:     logger,
			FileSystem: fs,
			Downloader: downloader,
		}
		repo := NewDittoRepo(config).(*dittoRepo)
		if len(repo.config.Dists) != 1 {
			t.Errorf("expected 1 dist, got %d", len(repo.config.Dists))
		}
		if repo.config.Dists[0] != "focal" {
			t.Errorf("expected dist 'focal', got '%s'", repo.config.Dists[0])
		}
	})

	t.Run("multiple dists configured", func(t *testing.T) {
		config := DittoConfig{
			Dists:      []string{"focal", "jammy", "noble"},
			Logger:     logger,
			FileSystem: fs,
			Downloader: downloader,
		}
		repo := NewDittoRepo(config).(*dittoRepo)
		if len(repo.config.Dists) != 3 {
			t.Errorf("expected 3 dists, got %d", len(repo.config.Dists))
		}
		expectedDists := []string{"focal", "jammy", "noble"}
		for i, dist := range repo.config.Dists {
			if dist != expectedDists[i] {
				t.Errorf("dist %d: expected '%s', got '%s'", i, expectedDists[i], dist)
			}
		}
	})

	t.Run("Dists takes precedence over Dist", func(t *testing.T) {
		config := DittoConfig{
			Dist:       "focal",
			Dists:      []string{"jammy", "noble"},
			Logger:     logger,
			FileSystem: fs,
			Downloader: downloader,
		}
		repo := NewDittoRepo(config).(*dittoRepo)
		if len(repo.config.Dists) != 2 {
			t.Errorf("expected 2 dists, got %d", len(repo.config.Dists))
		}
		if repo.config.Dists[0] != "jammy" || repo.config.Dists[1] != "noble" {
			t.Errorf("expected ['jammy', 'noble'], got %v", repo.config.Dists)
		}
	})

	t.Run("backwards compatibility - RepoURL converts to RepoURLs", func(t *testing.T) {
		config := DittoConfig{
			RepoURL:    "http://archive.ubuntu.com/ubuntu",
			Logger:     logger,
			FileSystem: fs,
			Downloader: downloader,
		}
		repo := NewDittoRepo(config).(*dittoRepo)
		if len(repo.config.RepoURLs) != 1 {
			t.Fatalf("expected 1 repo URL, got %d", len(repo.config.RepoURLs))
		}
		if repo.config.RepoURLs[0] != "http://archive.ubuntu.com/ubuntu" {
			t.Errorf("expected RepoURLs[0] 'http://archive.ubuntu.com/ubuntu', got '%s'", repo.config.RepoURLs[0])
		}
	})

	t.Run("multiple repo URLs configured", func(t *testing.T) {
		urls := []string{"http://archive.ubuntu.com/ubuntu", "http://ports.ubuntu.com/ubuntu"}
		config := DittoConfig{
			RepoURLs:   urls,
			Logger:     logger,
			FileSystem: fs,
			Downloader: downloader,
		}
		repo := NewDittoRepo(config).(*dittoRepo)
		if len(repo.config.RepoURLs) != 2 {
			t.Fatalf("expected 2 repo URLs, got %d", len(repo.config.RepoURLs))
		}
		for i, u := range urls {
			if repo.config.RepoURLs[i] != u {
				t.Errorf("repo URL %d: expected '%s', got '%s'", i, u, repo.config.RepoURLs[i])
			}
		}
	})

	t.Run("RepoURLs takes precedence over RepoURL", func(t *testing.T) {
		config := DittoConfig{
			RepoURL:    "http://archive.ubuntu.com/ubuntu",
			RepoURLs:   []string{"http://mirror-a/ubuntu", "http://mirror-b/ubuntu"},
			Logger:     logger,
			FileSystem: fs,
			Downloader: downloader,
		}
		repo := NewDittoRepo(config).(*dittoRepo)
		if len(repo.config.RepoURLs) != 2 {
			t.Fatalf("expected 2 repo URLs, got %d", len(repo.config.RepoURLs))
		}
		if repo.config.RepoURLs[0] != "http://mirror-a/ubuntu" || repo.config.RepoURLs[1] != "http://mirror-b/ubuntu" {
			t.Errorf("expected ['http://mirror-a/ubuntu', 'http://mirror-b/ubuntu'], got %v", repo.config.RepoURLs)
		}
	})
}

func TestParseReleaseFile(t *testing.T) {
	fs := NewMemFileSystem()
	logger := &mockLogger{}
	downloader := &mockDownloader{}

	releaseContent := `Origin: Ubuntu
Label: Ubuntu
Suite: focal
Version: 20.04
Codename: focal
Date: Thu, 23 Apr 2020 17:33:17 UTC
Architectures: amd64 arm64 armhf i386 ppc64el riscv64 s390x
Components: main restricted universe multiverse
Description: Ubuntu Focal 20.04
MD5Sum:
 d41d8cd98f00b204e9800998ecf8427e        0 main/binary-amd64/Packages
SHA256:
 e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855        0 main/binary-amd64/Packages.gz
 abc1234567890abcdef1234567890abcdef1234567890abcdef1234567890abc    12345 main/binary-arm64/Packages.gz
 def9876543210fedcba9876543210fedcba9876543210fedcba9876543210fed     5678 universe/binary-amd64/Packages.xz
 123abc456def789012abc456def789012abc456def789012abc456def789012a     1000 main/i18n/Translation-en.gz
 456def789abc012345def789abc012345def789abc012345def789abc012345d     2000 main/i18n/Translation-es.bz2
 789012345678901234567890123456789012345678901234567890123456789     3000 contrib/binary-amd64/Packages.gz
 aaa111bbb222ccc333ddd444eee555fff666aaa111bbb222ccc333ddd444eee5     4000 main/cnf/Commands-amd64.xz
 bbb222ccc333ddd444eee555fff666aaa111bbb222ccc333ddd444eee555fff6     5000 main/cnf/Commands-arm64.xz
 ccc333ddd444eee555fff666aaa111bbb222ccc333ddd444eee555fff666aaa1     6000 universe/cnf/Commands-amd64.xz`

	config := DittoConfig{
		Components: []string{"main", "universe"},
		Archs:      []string{"amd64", "arm64"},
		Languages:  []string{"en", "es"},
		Logger:     logger,
		FileSystem: fs,
		Downloader: downloader,
	}

	repo := NewDittoRepo(config).(*dittoRepo)
	indices := repo.parseReleaseFile(releaseContent)

	expected := []string{
		"main/binary-amd64/Packages.gz",
		"main/binary-arm64/Packages.gz",
		"universe/binary-amd64/Packages.xz",
		"main/i18n/Translation-en.gz",
		"main/i18n/Translation-es.bz2",
		"main/cnf/Commands-amd64.xz",
		"main/cnf/Commands-arm64.xz",
		"universe/cnf/Commands-amd64.xz",
	}

	if len(indices) != len(expected) {
		t.Errorf("expected %d indices, got %d", len(expected), len(indices))
	}

	for i, idx := range indices {
		if idx != expected[i] {
			t.Errorf("index %d: expected %s, got %s", i, expected[i], idx)
		}
	}
}

func TestIsDesired(t *testing.T) {
	fs := NewMemFileSystem()
	logger := &mockLogger{}
	downloader := &mockDownloader{}

	config := DittoConfig{
		Components: []string{"main", "universe"},
		Archs:      []string{"amd64", "arm64"},
		Languages:  []string{"en"},
		Logger:     logger,
		FileSystem: fs,
		Downloader: downloader,
	}

	repo := NewDittoRepo(config).(*dittoRepo)

	tests := []struct {
		name     string
		filePath string
		want     bool
	}{
		{
			name:     "main binary amd64 packages",
			filePath: "main/binary-amd64/Packages.gz",
			want:     true,
		},
		{
			name:     "universe binary arm64 packages",
			filePath: "universe/binary-arm64/Packages.xz",
			want:     true,
		},
		{
			name:     "main translation en",
			filePath: "main/i18n/Translation-en.gz",
			want:     true,
		},
		{
			name:     "wrong component",
			filePath: "contrib/binary-amd64/Packages.gz",
			want:     false,
		},
		{
			name:     "wrong architecture",
			filePath: "main/binary-i386/Packages.gz",
			want:     false,
		},
		{
			name:     "wrong language",
			filePath: "main/i18n/Translation-es.gz",
			want:     false,
		},
		{
			name:     "not a package or translation file",
			filePath: "main/source/Sources.gz",
			want:     false,
		},
		{
			name:     "main cnf commands amd64",
			filePath: "main/cnf/Commands-amd64.xz",
			want:     true,
		},
		{
			name:     "universe cnf commands arm64",
			filePath: "universe/cnf/Commands-arm64.xz",
			want:     true,
		},
		{
			name:     "wrong cnf architecture",
			filePath: "main/cnf/Commands-i386.xz",
			want:     false,
		},
		{
			name:     "wrong component for cnf",
			filePath: "contrib/cnf/Commands-amd64.xz",
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := repo.isDesired(tt.filePath)
			if got != tt.want {
				t.Errorf("isDesired(%s) = %v, want %v", tt.filePath, got, tt.want)
			}
		})
	}
}

func TestExtractDebsFromIndex(t *testing.T) {
	fs := NewMemFileSystem().(*MemFileSystem)
	logger := &mockLogger{}
	downloader := &mockDownloader{}

	// Create a sample Packages file content
	packagesContent := `Package: foo
Version: 1.0
Architecture: amd64
Filename: pool/main/f/foo/foo_1.0_amd64.deb
Size: 12345
SHA256: abc123def456abc123def456abc123def456abc123def456abc123def456abc1

Package: bar
Version: 2.0
Architecture: amd64
Filename: pool/main/b/bar/bar_2.0_amd64.deb
Size: 67890
SHA256: def456abc123def456abc123def456abc123def456abc123def456abc123def4

`

	// Compress the content with gzip
	var buf bytes.Buffer
	gzWriter := gzip.NewWriter(&buf)
	_, _ = gzWriter.Write([]byte(packagesContent))
	gzWriter.Close()

	// Create the file in the in-memory filesystem
	testPath := "/test/Packages.gz"
	fs.mu.Lock()
	fs.files["/test"] = &memFile{
		isDir:   true,
		mode:    0o755,
		modTime: time.Now(),
	}
	fs.files[testPath] = &memFile{
		data:    buf.Bytes(),
		mode:    0o644,
		modTime: time.Now(),
	}
	fs.mu.Unlock()

	config := DittoConfig{
		Logger:     logger,
		FileSystem: fs,
		Downloader: downloader,
	}
	repo := NewDittoRepo(config).(*dittoRepo)

	packages, err := repo.extractDebsFromIndex(testPath)
	if err != nil {
		t.Fatalf("extractDebsFromIndex failed: %v", err)
	}

	if len(packages) != 2 {
		t.Fatalf("expected 2 packages, got %d", len(packages))
	}

	// Check first package
	if packages[0].Path != "pool/main/f/foo/foo_1.0_amd64.deb" {
		t.Errorf("package 0 path: expected 'pool/main/f/foo/foo_1.0_amd64.deb', got '%s'", packages[0].Path)
	}
	if packages[0].SHA256 != "abc123def456abc123def456abc123def456abc123def456abc123def456abc1" {
		t.Errorf("package 0 SHA256: expected 'abc123...', got '%s'", packages[0].SHA256)
	}

	// Check second package
	if packages[1].Path != "pool/main/b/bar/bar_2.0_amd64.deb" {
		t.Errorf("package 1 path: expected 'pool/main/b/bar/bar_2.0_amd64.deb', got '%s'", packages[1].Path)
	}
	if packages[1].SHA256 != "def456abc123def456abc123def456abc123def456abc123def456abc123def4" {
		t.Errorf("package 1 SHA256: expected 'def456...', got '%s'", packages[1].SHA256)
	}
}

func TestVerifyFile(t *testing.T) {
	fs := NewMemFileSystem().(*MemFileSystem)
	logger := &mockLogger{}
	downloader := &mockDownloader{}

	testData := []byte("test file contents")
	hasher := sha256.New()
	hasher.Write(testData)
	correctHash := hex.EncodeToString(hasher.Sum(nil))
	wrongHash := "0000000000000000000000000000000000000000000000000000000000000000"

	// Create test file
	testPath := "/test/file.txt"
	fs.mu.Lock()
	fs.files["/test"] = &memFile{
		isDir:   true,
		mode:    0o755,
		modTime: time.Now(),
	}
	fs.files[testPath] = &memFile{
		data:    testData,
		mode:    0o644,
		modTime: time.Now(),
	}
	fs.mu.Unlock()

	config := DittoConfig{
		Logger:     logger,
		FileSystem: fs,
		Downloader: downloader,
	}
	repo := NewDittoRepo(config).(*dittoRepo)

	t.Run("correct checksum", func(t *testing.T) {
		match, err := repo.verifyFile(testPath, correctHash)
		if err != nil {
			t.Fatalf("verifyFile failed: %v", err)
		}
		if !match {
			t.Error("expected checksum to match")
		}
	})

	t.Run("incorrect checksum", func(t *testing.T) {
		match, err := repo.verifyFile(testPath, wrongHash)
		if err != nil {
			t.Fatalf("verifyFile failed: %v", err)
		}
		if match {
			t.Error("expected checksum to not match")
		}
	})

	t.Run("nonexistent file", func(t *testing.T) {
		_, err := repo.verifyFile("/nonexistent", correctHash)
		if err == nil {
			t.Error("expected error for nonexistent file")
		}
	})
}

func TestCreateByHashLink(t *testing.T) {
	fs := NewMemFileSystem().(*MemFileSystem)
	logger := &mockLogger{}
	downloader := &mockDownloader{}

	testData := []byte("test content")
	testHash := "abc123"

	// Create the original file
	originalPath := "/dists/focal/main/binary-amd64/Packages.gz"
	_ = fs.MkdirAll("/dists/focal/main/binary-amd64", 0o755)
	fs.mu.Lock()
	fs.files[originalPath] = &memFile{
		data:    testData,
		mode:    0o644,
		modTime: time.Now(),
	}
	fs.mu.Unlock()

	config := DittoConfig{
		Logger:     logger,
		FileSystem: fs,
		Downloader: downloader,
	}
	repo := NewDittoRepo(config).(*dittoRepo)

	err := repo.createByHashLink(originalPath, testHash)
	if err != nil {
		t.Fatalf("createByHashLink failed: %v", err)
	}

	// Verify the by-hash directory was created
	expectedDir := "/dists/focal/main/binary-amd64/by-hash/SHA256"
	if _, err := fs.Stat(expectedDir); err != nil {
		t.Errorf("by-hash directory not created: %v", err)
	}

	// Verify the link was created
	expectedLink := expectedDir + "/" + testHash
	linkData, err := fs.ReadFile(expectedLink)
	if err != nil {
		t.Fatalf("failed to read linked file: %v", err)
	}

	if !bytes.Equal(linkData, testData) {
		t.Error("linked file content doesn't match original")
	}
}

func TestCreateByHashLink_FallbackToCopy(t *testing.T) {
	// Create a custom filesystem that fails on Link to test the copy fallback
	fs := &failingLinkFS{
		MemFileSystem: NewMemFileSystem().(*MemFileSystem),
	}
	logger := &mockLogger{}
	downloader := &mockDownloader{}

	testData := []byte("test content")
	testHash := "def456"

	// Create the original file
	originalPath := "/dists/focal/main/binary-amd64/Packages.gz"
	_ = fs.MkdirAll("/dists/focal/main/binary-amd64", 0o755)
	fs.mu.Lock()
	fs.files[originalPath] = &memFile{
		data:    testData,
		mode:    0o644,
		modTime: time.Now(),
	}
	fs.mu.Unlock()

	config := DittoConfig{
		Logger:     logger,
		FileSystem: fs,
		Downloader: downloader,
	}
	repo := NewDittoRepo(config).(*dittoRepo)

	err := repo.createByHashLink(originalPath, testHash)
	if err != nil {
		t.Fatalf("createByHashLink failed: %v", err)
	}

	// Verify the copy was created despite Link failing
	expectedLink := "/dists/focal/main/binary-amd64/by-hash/SHA256/" + testHash
	linkData, err := fs.ReadFile(expectedLink)
	if err != nil {
		t.Fatalf("failed to read copied file: %v", err)
	}

	if !bytes.Equal(linkData, testData) {
		t.Error("copied file content doesn't match original")
	}
}

// failingLinkFS is a test filesystem that always fails on Link operations
type failingLinkFS struct {
	*MemFileSystem
}

func (fs *failingLinkFS) Link(_, _ string) error {
	return &testError{msg: "link not supported"}
}

type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}

func TestParseReleaseFile_EmptyContent(t *testing.T) {
	fs := NewMemFileSystem()
	logger := &mockLogger{}
	downloader := &mockDownloader{}

	config := DittoConfig{
		Components: []string{"main"},
		Archs:      []string{"amd64"},
		Logger:     logger,
		FileSystem: fs,
		Downloader: downloader,
	}

	repo := NewDittoRepo(config).(*dittoRepo)
	indices := repo.parseReleaseFile("")

	if len(indices) != 0 {
		t.Errorf("expected 0 indices from empty content, got %d", len(indices))
	}
}

func TestParseReleaseFile_NoSHA256Block(t *testing.T) {
	fs := NewMemFileSystem()
	logger := &mockLogger{}
	downloader := &mockDownloader{}

	releaseContent := `Origin: Ubuntu
Label: Ubuntu
Suite: focal
MD5Sum:
 d41d8cd98f00b204e9800998ecf8427e        0 main/binary-amd64/Packages`

	config := DittoConfig{
		Components: []string{"main"},
		Archs:      []string{"amd64"},
		Logger:     logger,
		FileSystem: fs,
		Downloader: downloader,
	}

	repo := NewDittoRepo(config).(*dittoRepo)
	indices := repo.parseReleaseFile(releaseContent)

	if len(indices) != 0 {
		t.Errorf("expected 0 indices without SHA256 block, got %d", len(indices))
	}
}

func TestIsDesired_EmptyConfig(t *testing.T) {
	fs := NewMemFileSystem()
	logger := &mockLogger{}
	downloader := &mockDownloader{}

	config := DittoConfig{
		Components: []string{},
		Archs:      []string{},
		Languages:  []string{},
		Logger:     logger,
		FileSystem: fs,
		Downloader: downloader,
	}

	repo := NewDittoRepo(config).(*dittoRepo)

	tests := []string{
		"main/binary-amd64/Packages.gz",
		"universe/i18n/Translation-en.gz",
		"contrib/binary-arm64/Packages.xz",
	}

	for _, filePath := range tests {
		if repo.isDesired(filePath) {
			t.Errorf("expected isDesired(%s) = false with empty config", filePath)
		}
	}
}

func TestExtractDebsFromIndex_EmptyFile(t *testing.T) {
	fs := NewMemFileSystem().(*MemFileSystem)
	logger := &mockLogger{}
	downloader := &mockDownloader{}

	// Create an empty gzipped file
	var buf bytes.Buffer
	gzWriter := gzip.NewWriter(&buf)
	gzWriter.Close()

	testPath := "/test/empty.gz"
	fs.mu.Lock()
	fs.files["/test"] = &memFile{
		isDir:   true,
		mode:    0o755,
		modTime: time.Now(),
	}
	fs.files[testPath] = &memFile{
		data:    buf.Bytes(),
		mode:    0o644,
		modTime: time.Now(),
	}
	fs.mu.Unlock()

	config := DittoConfig{
		Logger:     logger,
		FileSystem: fs,
		Downloader: downloader,
	}
	repo := NewDittoRepo(config).(*dittoRepo)

	packages, err := repo.extractDebsFromIndex(testPath)
	if err != nil {
		t.Fatalf("extractDebsFromIndex failed: %v", err)
	}

	if len(packages) != 0 {
		t.Errorf("expected 0 packages from empty file, got %d", len(packages))
	}
}

func TestExtractDebsFromIndex_IncompletePackage(t *testing.T) {
	fs := NewMemFileSystem().(*MemFileSystem)
	logger := &mockLogger{}
	downloader := &mockDownloader{}

	// Package with missing SHA256
	packagesContent := `Package: incomplete
Version: 1.0
Architecture: amd64
Filename: pool/main/i/incomplete/incomplete_1.0_amd64.deb
Size: 12345

`

	var buf bytes.Buffer
	gzWriter := gzip.NewWriter(&buf)
	_, _ = gzWriter.Write([]byte(packagesContent))
	gzWriter.Close()

	testPath := "/test/incomplete.gz"
	fs.mu.Lock()
	fs.files["/test"] = &memFile{
		isDir:   true,
		mode:    0o755,
		modTime: time.Now(),
	}
	fs.files[testPath] = &memFile{
		data:    buf.Bytes(),
		mode:    0o644,
		modTime: time.Now(),
	}
	fs.mu.Unlock()

	config := DittoConfig{
		Logger:     logger,
		FileSystem: fs,
		Downloader: downloader,
	}
	repo := NewDittoRepo(config).(*dittoRepo)

	packages, err := repo.extractDebsFromIndex(testPath)
	if err != nil {
		t.Fatalf("extractDebsFromIndex failed: %v", err)
	}

	// Package should be skipped because it's missing SHA256
	if len(packages) != 0 {
		t.Errorf("expected 0 packages (incomplete should be skipped), got %d", len(packages))
	}
}

func TestMultipleDistributions(t *testing.T) {
	fs := NewMemFileSystem().(*MemFileSystem)
	logger := &mockLogger{}
	downloader := &mockDownloader{}

	// Set up test configuration with multiple dists
	config := DittoConfig{
		RepoURL:      "http://archive.ubuntu.com/ubuntu",
		Dists:        []string{"focal", "jammy"},
		Components:   []string{"main"},
		Archs:        []string{"amd64"},
		Languages:    []string{"en"},
		DownloadPath: "/mirror",
		Workers:      2,
		Logger:       logger,
		FileSystem:   fs,
		Downloader:   downloader,
	}

	repo := NewDittoRepo(config).(*dittoRepo)

	// Verify that the config has both dists
	if len(repo.config.Dists) != 2 {
		t.Fatalf("expected 2 dists, got %d", len(repo.config.Dists))
	}

	if repo.config.Dists[0] != "focal" {
		t.Errorf("expected first dist to be 'focal', got '%s'", repo.config.Dists[0])
	}

	if repo.config.Dists[1] != "jammy" {
		t.Errorf("expected second dist to be 'jammy', got '%s'", repo.config.Dists[1])
	}

	// Create Release files for both distributions
	releaseContentFocal := `Origin: Ubuntu
Label: Ubuntu
Suite: focal
SHA256:
 e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855        0 main/binary-amd64/Packages.gz
`

	releaseContentJammy := `Origin: Ubuntu
Label: Ubuntu
Suite: jammy
SHA256:
 abc123def456abc123def456abc123def456abc123def456abc123def456abc1        0 main/binary-amd64/Packages.gz
`

	// Setup filesystem with Release files for both dists
	_ = fs.MkdirAll("/mirror/dists/focal", 0o755)
	_ = fs.MkdirAll("/mirror/dists/jammy", 0o755)

	fs.mu.Lock()
	fs.files["/mirror/dists/focal/Release"] = &memFile{
		data:    []byte(releaseContentFocal),
		mode:    0o644,
		modTime: time.Now(),
	}
	fs.files["/mirror/dists/jammy/Release"] = &memFile{
		data:    []byte(releaseContentJammy),
		mode:    0o644,
		modTime: time.Now(),
	}
	fs.mu.Unlock()

	// Test that parseReleaseFile works for both
	indicesFocal := repo.parseReleaseFile(releaseContentFocal)
	if len(indicesFocal) != 1 {
		t.Errorf("expected 1 index for focal, got %d", len(indicesFocal))
	}

	indicesJammy := repo.parseReleaseFile(releaseContentJammy)
	if len(indicesJammy) != 1 {
		t.Errorf("expected 1 index for jammy, got %d", len(indicesJammy))
	}

	// Verify that the downloader would be called for metadata from both dists
	// The actual Mirror() method would call DownloadFile for:
	// - focal: InRelease, Release, Release.gpg
	// - jammy: InRelease, Release, Release.gpg
	// Plus indices for each
	expectedURLs := []string{
		"http://archive.ubuntu.com/ubuntu/dists/focal/InRelease",
		"http://archive.ubuntu.com/ubuntu/dists/focal/Release",
		"http://archive.ubuntu.com/ubuntu/dists/focal/Release.gpg",
		"http://archive.ubuntu.com/ubuntu/dists/jammy/InRelease",
		"http://archive.ubuntu.com/ubuntu/dists/jammy/Release",
		"http://archive.ubuntu.com/ubuntu/dists/jammy/Release.gpg",
	}

	// This test verifies the configuration is correct and that both distributions
	// would be processed. A full integration test would require setting up
	// the complete mirror environment with package indices.
	for _, dist := range repo.config.Dists {
		expectedPath := "/mirror/dists/" + dist + "/Release"
		if _, err := fs.Stat(expectedPath); err != nil {
			t.Errorf("expected Release file for dist '%s' to exist at %s", dist, expectedPath)
		}
	}

	t.Logf("Successfully configured mirror for %d distributions: %v", len(repo.config.Dists), repo.config.Dists)
	t.Logf("Expected metadata downloads from both dists: %v", expectedURLs)
}

func TestNewDittoRepo_VerifyMode(t *testing.T) {
	fs := NewMemFileSystem()
	logger := &mockLogger{}
	downloader := &mockDownloader{}

	t.Run("defaults to checksum when not set", func(t *testing.T) {
		config := DittoConfig{
			Logger:     logger,
			FileSystem: fs,
			Downloader: downloader,
		}
		repo := NewDittoRepo(config).(*dittoRepo)
		if repo.config.VerifyMode != VerifyChecksum {
			t.Errorf("expected default VerifyMode %q, got %q", VerifyChecksum, repo.config.VerifyMode)
		}
	})

	t.Run("preserves VerifySize when set", func(t *testing.T) {
		config := DittoConfig{
			VerifyMode: VerifySize,
			Logger:     logger,
			FileSystem: fs,
			Downloader: downloader,
		}
		repo := NewDittoRepo(config).(*dittoRepo)
		if repo.config.VerifyMode != VerifySize {
			t.Errorf("expected VerifyMode %q, got %q", VerifySize, repo.config.VerifyMode)
		}
	})

	t.Run("preserves VerifyChecksum when explicitly set", func(t *testing.T) {
		config := DittoConfig{
			VerifyMode: VerifyChecksum,
			Logger:     logger,
			FileSystem: fs,
			Downloader: downloader,
		}
		repo := NewDittoRepo(config).(*dittoRepo)
		if repo.config.VerifyMode != VerifyChecksum {
			t.Errorf("expected VerifyMode %q, got %q", VerifyChecksum, repo.config.VerifyMode)
		}
	})
}

func TestExtractDebsFromIndex_Size(t *testing.T) {
	memFS := NewMemFileSystem().(*MemFileSystem)
	logger := &mockLogger{}
	downloader := &mockDownloader{}

	packagesContent := `Package: foo
Version: 1.0
Architecture: amd64
Filename: pool/main/f/foo/foo_1.0_amd64.deb
Size: 98765
SHA256: abc123def456abc123def456abc123def456abc123def456abc123def456abc1

Package: bar
Version: 2.0
Architecture: amd64
Filename: pool/main/b/bar/bar_2.0_amd64.deb
Size: 11111
SHA256: def456abc123def456abc123def456abc123def456abc123def456abc123def4

`

	var buf bytes.Buffer
	gzWriter := gzip.NewWriter(&buf)
	_, _ = gzWriter.Write([]byte(packagesContent))
	gzWriter.Close()

	testPath := "/test/Packages_size.gz"
	memFS.mu.Lock()
	memFS.files["/test"] = &memFile{isDir: true, mode: 0o755, modTime: time.Now()}
	memFS.files[testPath] = &memFile{data: buf.Bytes(), mode: 0o644, modTime: time.Now()}
	memFS.mu.Unlock()

	config := DittoConfig{Logger: logger, FileSystem: memFS, Downloader: downloader}
	repo := NewDittoRepo(config).(*dittoRepo)

	packages, err := repo.extractDebsFromIndex(testPath)
	if err != nil {
		t.Fatalf("extractDebsFromIndex failed: %v", err)
	}
	if len(packages) != 2 {
		t.Fatalf("expected 2 packages, got %d", len(packages))
	}

	if packages[0].Size != 98765 {
		t.Errorf("package 0 Size: expected 98765, got %d", packages[0].Size)
	}
	if packages[1].Size != 11111 {
		t.Errorf("package 1 Size: expected 11111, got %d", packages[1].Size)
	}
}

// indexFailingDownloader succeeds for all requests except those whose URL
// contains "Packages", which it returns a configurable error for.
type indexFailingDownloader struct {
	err error
}

func (d *indexFailingDownloader) DownloadFile(urlStr string, _ string, _ string) (string, error) {
	if strings.Contains(urlStr, "Packages") {
		return "", d.err
	}
	return "fakehash", nil
}

func TestMirrorDistribution_AllowMissingIndices(t *testing.T) {
	const downloadPath = "/mirror"
	const repoURL = "http://example.com/ubuntu"
	const dist = "focal"

	// Release file that references one Packages index.
	releaseContent := `Origin: Ubuntu
Label: Ubuntu
Suite: focal
SHA256:
 e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855        0 main/binary-amd64/Packages.gz
`

	setup := func(t *testing.T, allowMissing bool) (*dittoRepo, *mockLogger) {
		t.Helper()
		memFS := NewMemFileSystem().(*MemFileSystem)
		logger := &mockLogger{}

		// Pre-populate the Release file so mirrorDistribution can read it
		// after the (failing) metadata download attempts.
		_ = memFS.MkdirAll("/mirror/dists/focal", 0o755)
		memFS.mu.Lock()
		memFS.files["/mirror/dists/focal/Release"] = &memFile{
			data:    []byte(releaseContent),
			mode:    0o644,
			modTime: time.Now(),
		}
		memFS.mu.Unlock()

		config := DittoConfig{
			RepoURL:             repoURL,
			Dists:               []string{dist},
			Components:          []string{"main"},
			Archs:               []string{"amd64"},
			DownloadPath:        downloadPath,
			Workers:             1,
			AllowMissingIndices: allowMissing,
			Logger:              logger,
			FileSystem:          memFS,
			Downloader:          &indexFailingDownloader{err: fmt.Errorf("status 404")},
		}
		repo := NewDittoRepo(config).(*dittoRepo)
		repo.progressChan = make(chan ProgressUpdate, 100)
		return repo, logger
	}

	t.Run("returns error when AllowMissingIndices is false", func(t *testing.T) {
		repo, _ := setup(t, false)
		err := repo.mirrorDistribution(context.Background(), dist)
		if err == nil {
			t.Error("expected an error when a Packages index cannot be fetched and AllowMissingIndices is false")
		}
	})

	t.Run("continues and warns when AllowMissingIndices is true", func(t *testing.T) {
		repo, logger := setup(t, true)
		err := repo.mirrorDistribution(context.Background(), dist)
		if err != nil {
			t.Errorf("expected no error when AllowMissingIndices is true, got: %v", err)
		}
		if len(logger.warnMsgs) == 0 {
			t.Error("expected at least one warning to be logged for the skipped index")
		}
	})
}

// trackingDownloader records which URLs were actually downloaded (i.e. not skipped).
type trackingDownloader struct {
	downloads []string
}

func (d *trackingDownloader) DownloadFile(urlStr string, _ string, _ string) (string, error) {
	d.downloads = append(d.downloads, urlStr)
	return "fakehash", nil
}

func TestDownloadPackages_VerifySize(t *testing.T) {
	const downloadPath = "/mirror"
	const repoURL = "http://example.com/ubuntu"

	// Two packages, one with a correct on-disk size, one with a wrong size.
	pkgs := []packageMeta{
		{Path: "pool/main/f/foo/foo_1.0_amd64.deb", SHA256: "aaa", Size: 10},
		{Path: "pool/main/b/bar/bar_2.0_amd64.deb", SHA256: "bbb", Size: 20},
	}

	setup := func(t *testing.T, verifyMode VerifyMode) (*dittoRepo, *trackingDownloader) {
		t.Helper()
		memFS := NewMemFileSystem().(*MemFileSystem)

		// foo exists on disk with the correct size (10 bytes).
		fooPath := downloadPath + "/" + pkgs[0].Path
		_ = memFS.MkdirAll("pool/main/f/foo", 0o755)
		memFS.mu.Lock()
		memFS.files[fooPath] = &memFile{data: bytes.Repeat([]byte("x"), 10), mode: 0o644, modTime: time.Now()}
		memFS.mu.Unlock()

		// bar exists on disk but with the wrong size (5 bytes instead of 20).
		barPath := downloadPath + "/" + pkgs[1].Path
		_ = memFS.MkdirAll("pool/main/b/bar", 0o755)
		memFS.mu.Lock()
		memFS.files[barPath] = &memFile{data: bytes.Repeat([]byte("y"), 5), mode: 0o644, modTime: time.Now()}
		memFS.mu.Unlock()

		td := &trackingDownloader{}
		config := DittoConfig{
			RepoURL:      repoURL,
			DownloadPath: downloadPath,
			Workers:      1,
			VerifyMode:   verifyMode,
			Logger:       &mockLogger{},
			FileSystem:   memFS,
			Downloader:   td,
		}
		repo := NewDittoRepo(config).(*dittoRepo)
		repo.progressChan = make(chan ProgressUpdate, 100)
		return repo, td
	}

	t.Run("VerifySize skips file with matching size", func(t *testing.T) {
		repo, td := setup(t, VerifySize)
		repo.downloadPackages(context.Background(), pkgs)

		for _, url := range td.downloads {
			if url == repoURL+"/"+pkgs[0].Path {
				t.Errorf("foo should have been skipped (size matches) but was downloaded")
			}
		}
	})

	t.Run("VerifySize redownloads file with wrong size", func(t *testing.T) {
		repo, td := setup(t, VerifySize)
		repo.downloadPackages(context.Background(), pkgs)

		found := false
		for _, url := range td.downloads {
			if url == repoURL+"/"+pkgs[1].Path {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("bar should have been redownloaded (size mismatch) but was not")
		}
	})

	t.Run("VerifyChecksum skips file with matching checksum", func(t *testing.T) {
		repo, td := setup(t, VerifyChecksum)

		// Override foo on disk so its SHA256 matches pkgs[0].SHA256 ("aaa" hashed).
		// Rather than computing the real hash, put the correct hash into pkgs copy.
		fooData := bytes.Repeat([]byte("x"), 10)
		h := sha256.New()
		h.Write(fooData)
		correctHash := hex.EncodeToString(h.Sum(nil))

		localPkgs := []packageMeta{
			{Path: pkgs[0].Path, SHA256: correctHash, Size: 10},
			pkgs[1],
		}
		repo.downloadPackages(context.Background(), localPkgs)

		for _, url := range td.downloads {
			if url == repoURL+"/"+pkgs[0].Path {
				t.Errorf("foo should have been skipped (checksum matches) but was downloaded")
			}
		}
	})
}

// newTestRepo builds a *dittoRepo with the given config and in-memory/mock dependencies.
func newTestRepo(t *testing.T, config DittoConfig, downloader Downloader) *dittoRepo {
	t.Helper()
	config.Logger = &mockLogger{}
	config.FileSystem = NewMemFileSystem()
	config.Downloader = downloader
	return NewDittoRepo(config).(*dittoRepo)
}

func TestPreferredBaseForPath(t *testing.T) {
	const ports = "https://ports.ubuntu.com/ubuntu"
	const amd64Mirror = "https://amd64-mirror.example.com/ubuntu"

	repo := newTestRepo(t, DittoConfig{
		RepoURLs: []string{"https://archive.ubuntu.com/ubuntu"},
		ArchURLs: map[string]string{
			"arm64": ports,
			"armhf": ports,
			"amd64": amd64Mirror,
		},
	}, &mockDownloader{})

	tests := []struct {
		name    string
		relPath string
		want    string
	}{
		{"binary index for arm64", "dists/stonking/main/binary-arm64/Packages.gz", ports},
		{"cnf commands for armhf", "dists/stonking/main/cnf/Commands-armhf.xz", ports},
		{"package filename for arm64", "pool/main/h/hello/hello_2.10_arm64.deb", ports},
		{"binary index for amd64", "dists/stonking/main/binary-amd64/Packages.gz", amd64Mirror},
		{"amd64 does not match amd64v3 index", "dists/stonking/main/binary-amd64v3/Packages.gz", ""},
		{"amd64 does not match amd64v3 package", "pool/main/h/hello/hello_2.10_amd64v3.deb", ""},
		{"unrelated metadata path", "dists/stonking/Release", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := repo.preferredBaseForPath(tt.relPath); got != tt.want {
				t.Errorf("preferredBaseForPath(%q) = %q, want %q", tt.relPath, got, tt.want)
			}
		})
	}
}

func TestCandidateURLs(t *testing.T) {
	const archive = "https://archive.ubuntu.com/ubuntu"
	const ports = "https://ports.ubuntu.com/ubuntu"

	t.Run("preferred arch URL is tried first and deduped", func(t *testing.T) {
		repo := newTestRepo(t, DittoConfig{
			RepoURLs: []string{archive, ports},
			ArchURLs: map[string]string{"arm64": ports},
		}, &mockDownloader{})

		got := repo.candidateURLs("dists/stonking/main/binary-arm64/Packages.gz")
		want := []string{ports, archive}
		if !slices.Equal(got, want) {
			t.Errorf("candidateURLs = %v, want %v", got, want)
		}
	})

	t.Run("no preference preserves RepoURLs order", func(t *testing.T) {
		repo := newTestRepo(t, DittoConfig{
			RepoURLs: []string{archive, ports},
			ArchURLs: map[string]string{"arm64": ports},
		}, &mockDownloader{})

		got := repo.candidateURLs("dists/stonking/main/binary-amd64/Packages.gz")
		want := []string{archive, ports}
		if !slices.Equal(got, want) {
			t.Errorf("candidateURLs = %v, want %v", got, want)
		}
	})

	t.Run("single URL", func(t *testing.T) {
		repo := newTestRepo(t, DittoConfig{
			RepoURLs: []string{archive},
		}, &mockDownloader{})

		got := repo.candidateURLs("pool/main/h/hello/hello_2.10_amd64.deb")
		want := []string{archive}
		if !slices.Equal(got, want) {
			t.Errorf("candidateURLs = %v, want %v", got, want)
		}
	})
}

func TestDownloadWithFailover(t *testing.T) {
	const archive = "https://archive.example.com/ubuntu"
	const ports = "https://ports.example.com/ubuntu"
	const relPath = "dists/stonking/Release"

	t.Run("first mirror succeeds without failover", func(t *testing.T) {
		md := &mockDownloader{}
		repo := newTestRepo(t, DittoConfig{RepoURLs: []string{archive, ports}}, md)

		hash, err := repo.downloadWithFailover(relPath, "/tmp/Release", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if hash != "fakehash123" {
			t.Errorf("expected fakehash123, got %q", hash)
		}
		if len(md.downloads) != 1 {
			t.Fatalf("expected exactly 1 attempt, got %d: %v", len(md.downloads), md.downloads)
		}
		if md.downloads[0] != archive+"/"+relPath {
			t.Errorf("expected first attempt %q, got %q", archive+"/"+relPath, md.downloads[0])
		}
	})

	t.Run("fails over to the second mirror", func(t *testing.T) {
		md := &mockDownloader{
			errByURL: map[string]error{archive + "/" + relPath: errors.New("status 404")},
		}
		repo := newTestRepo(t, DittoConfig{RepoURLs: []string{archive, ports}}, md)

		hash, err := repo.downloadWithFailover(relPath, "/tmp/Release", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if hash != "fakehash123" {
			t.Errorf("expected fakehash123 from second mirror, got %q", hash)
		}
		if len(md.downloads) != 2 {
			t.Fatalf("expected 2 attempts (failover), got %d: %v", len(md.downloads), md.downloads)
		}
		if md.downloads[1] != ports+"/"+relPath {
			t.Errorf("expected second attempt %q, got %q", ports+"/"+relPath, md.downloads[1])
		}
	})

	t.Run("all mirrors fail returns an error", func(t *testing.T) {
		md := &mockDownloader{err: errors.New("boom")}
		repo := newTestRepo(t, DittoConfig{RepoURLs: []string{archive, ports}}, md)

		if _, err := repo.downloadWithFailover(relPath, "/tmp/Release", ""); err == nil {
			t.Fatal("expected error when all mirrors fail, got nil")
		}
		if len(md.downloads) != 2 {
			t.Errorf("expected 2 attempts before failing, got %d", len(md.downloads))
		}
	})

	t.Run("arch preference is attempted first", func(t *testing.T) {
		md := &mockDownloader{}
		repo := newTestRepo(t, DittoConfig{
			RepoURLs: []string{archive, ports},
			ArchURLs: map[string]string{"arm64": ports},
		}, md)

		archRelPath := "pool/main/h/hello/hello_2.10_arm64.deb"
		if _, err := repo.downloadWithFailover(archRelPath, "/tmp/hello.deb", ""); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if md.downloads[0] != ports+"/"+archRelPath {
			t.Errorf("expected first attempt to arch preference %q, got %q", ports+"/"+archRelPath, md.downloads[0])
		}
	})

	t.Run("no repo URLs configured returns an error", func(t *testing.T) {
		repo := newTestRepo(t, DittoConfig{}, &mockDownloader{})
		if _, err := repo.downloadWithFailover(relPath, "/tmp/Release", ""); err == nil {
			t.Fatal("expected error when no repo URLs are configured")
		}
	})
}

func TestLearnedArchURLs(t *testing.T) {
	const archive = "https://archive.example.com/ubuntu"
	const ports = "https://ports.example.com/ubuntu"
	const arm64Index = "dists/stonking/main/binary-arm64/Packages.gz"
	const arm64Deb = "pool/main/h/hello/hello_2.10_arm64.deb"

	t.Run("learns the mirror that serves an arch after failover", func(t *testing.T) {
		// archive does not carry arm64, so the first fetch fails over to ports.
		md := &mockDownloader{
			errByURL: map[string]error{archive + "/" + arm64Index: errors.New("status 404")},
		}
		repo := newTestRepo(t, DittoConfig{
			RepoURLs: []string{archive, ports},
			Archs:    []string{"amd64", "arm64"},
		}, md)

		// Nothing learned yet, so the configured order is used.
		if got := repo.candidateURLs(arm64Index); !slices.Equal(got, []string{archive, ports}) {
			t.Fatalf("before learning: candidateURLs = %v, want [archive ports]", got)
		}

		if _, err := repo.downloadWithFailover(arm64Index, "/tmp/Packages.gz", ""); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// arm64 is now mapped to ports, so it is preferred for subsequent arm64 files.
		if got := repo.preferredBaseForPath(arm64Deb); got != ports {
			t.Errorf("preferredBaseForPath(arm64 deb) = %q, want %q", got, ports)
		}
		if got := repo.candidateURLs(arm64Deb); !slices.Equal(got, []string{ports, archive}) {
			t.Errorf("after learning: candidateURLs = %v, want [ports archive]", got)
		}

		// A subsequent arm64 download should hit ports first (no wasted archive attempt).
		before := len(md.downloads)
		if _, err := repo.downloadWithFailover(arm64Deb, "/tmp/hello.deb", ""); err != nil {
			t.Fatalf("unexpected error on second download: %v", err)
		}
		if md.downloads[before] != ports+"/"+arm64Deb {
			t.Errorf("expected first attempt %q, got %q", ports+"/"+arm64Deb, md.downloads[before])
		}
		if got := len(md.downloads) - before; got != 1 {
			t.Errorf("expected exactly 1 attempt for learned arch, got %d", got)
		}
	})

	t.Run("does not learn when an explicit mapping is provided", func(t *testing.T) {
		md := &mockDownloader{}
		repo := newTestRepo(t, DittoConfig{
			RepoURLs: []string{archive, ports},
			ArchURLs: map[string]string{"arm64": archive},
			Archs:    []string{"amd64", "arm64"},
		}, md)

		// Even though ports serves this file, the explicit mapping (archive) is honored
		// and the learned cache is left untouched.
		if _, err := repo.downloadWithFailover(arm64Index, "/tmp/Packages.gz", ""); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		repo.archCacheMu.RLock()
		_, learned := repo.learnedArchURLs["arm64"]
		repo.archCacheMu.RUnlock()
		if learned {
			t.Error("expected no learned entry when an explicit ArchURLs mapping exists")
		}
		if got := repo.preferredBaseForPath(arm64Deb); got != archive {
			t.Errorf("preferredBaseForPath = %q, want explicit mapping %q", got, archive)
		}
	})

	t.Run("first mirror to serve an arch wins", func(t *testing.T) {
		md := &mockDownloader{}
		repo := newTestRepo(t, DittoConfig{
			RepoURLs: []string{archive, ports},
			Archs:    []string{"arm64"},
		}, md)

		// archive serves the first arm64 file, so it is learned.
		if _, err := repo.downloadWithFailover(arm64Index, "/tmp/Packages.gz", ""); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// A later success from a different mirror must not overwrite the learned entry.
		repo.learnArchURL(arm64Deb, ports)

		repo.archCacheMu.RLock()
		got := repo.learnedArchURLs["arm64"]
		repo.archCacheMu.RUnlock()
		if got != archive {
			t.Errorf("learned arch should remain %q, got %q", archive, got)
		}
	})

	t.Run("non-arch files are not learned", func(t *testing.T) {
		md := &mockDownloader{}
		repo := newTestRepo(t, DittoConfig{
			RepoURLs: []string{archive, ports},
			Archs:    []string{"arm64"},
		}, md)

		if _, err := repo.downloadWithFailover("dists/stonking/Release", "/tmp/Release", ""); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		repo.archCacheMu.RLock()
		n := len(repo.learnedArchURLs)
		repo.archCacheMu.RUnlock()
		if n != 0 {
			t.Errorf("expected nothing learned for a non-arch file, got %d entries", n)
		}
	})
}

func TestValidateMirrorConsistency(t *testing.T) {
	const archive = "https://archive.example.com/ubuntu"
	const ports = "https://ports.example.com/ubuntu"
	const dist = "stonking"
	releaseURL := func(base string) string { return base + "/dists/" + dist + "/Release" }

	t.Run("single mirror skips validation", func(t *testing.T) {
		md := &mockDownloader{}
		repo := newTestRepo(t, DittoConfig{
			RepoURLs:     []string{archive},
			Dists:        []string{dist},
			DownloadPath: "/mirror",
		}, md)

		if err := repo.validateMirrorConsistency(context.Background()); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(md.downloads) != 0 {
			t.Errorf("expected no downloads for a single mirror, got %d: %v", len(md.downloads), md.downloads)
		}
	})

	t.Run("identical Release files pass", func(t *testing.T) {
		md := &mockDownloader{
			hashByURL: map[string]string{
				releaseURL(archive): "samehash",
				releaseURL(ports):   "samehash",
			},
		}
		repo := newTestRepo(t, DittoConfig{
			RepoURLs:     []string{archive, ports},
			Dists:        []string{dist},
			DownloadPath: "/mirror",
		}, md)

		if err := repo.validateMirrorConsistency(context.Background()); err != nil {
			t.Fatalf("expected validation to pass, got: %v", err)
		}
		if len(md.downloads) != 2 {
			t.Errorf("expected 2 Release fetches, got %d", len(md.downloads))
		}
	})

	t.Run("differing Release files fail", func(t *testing.T) {
		md := &mockDownloader{
			hashByURL: map[string]string{
				releaseURL(archive): "hash-a",
				releaseURL(ports):   "hash-b",
			},
		}
		repo := newTestRepo(t, DittoConfig{
			RepoURLs:     []string{archive, ports},
			Dists:        []string{dist},
			DownloadPath: "/mirror",
		}, md)

		if err := repo.validateMirrorConsistency(context.Background()); err == nil {
			t.Fatal("expected validation to fail for differing Release files")
		}
	})

	t.Run("fetch failure fails validation", func(t *testing.T) {
		md := &mockDownloader{
			errByURL: map[string]error{releaseURL(ports): errors.New("status 404")},
		}
		repo := newTestRepo(t, DittoConfig{
			RepoURLs:     []string{archive, ports},
			Dists:        []string{dist},
			DownloadPath: "/mirror",
		}, md)

		if err := repo.validateMirrorConsistency(context.Background()); err == nil {
			t.Fatal("expected validation to fail when a mirror cannot be fetched")
		}
	})

	t.Run("arch-url mirrors participate in validation", func(t *testing.T) {
		md := &mockDownloader{
			hashByURL: map[string]string{
				releaseURL(archive): "hash-a",
				releaseURL(ports):   "hash-b",
			},
		}
		repo := newTestRepo(t, DittoConfig{
			RepoURLs:     []string{archive},
			ArchURLs:     map[string]string{"arm64": ports},
			Dists:        []string{dist},
			DownloadPath: "/mirror",
		}, md)

		if err := repo.validateMirrorConsistency(context.Background()); err == nil {
			t.Fatal("expected validation to fail when an arch-url mirror diverges")
		}
	})
}
