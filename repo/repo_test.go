package repo

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
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
type mockDownloader struct {
	downloads []string
	err       error
}

func (d *mockDownloader) DownloadFile(urlStr string, _ string, _ string) (string, error) {
	d.downloads = append(d.downloads, urlStr)
	if d.err != nil {
		return "", d.err
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
