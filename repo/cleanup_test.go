package repo

import (
	"path/filepath"
	"testing"
	"time"
)

func TestCleanupOrphanedPackages(t *testing.T) {
	fs := NewMemFileSystem().(*MemFileSystem)
	logger := &mockLogger{}
	downloader := &mockDownloader{}

	config := DittoConfig{
		DownloadPath: "/mirror",
		Logger:       logger,
		FileSystem:   fs,
		Downloader:   downloader,
	}

	repo := NewDittoRepo(config).(*dittoRepo)

	// Set up pool directory with some .deb files
	poolPath := "/mirror/pool/main/f/foo"
	if err := fs.MkdirAll(poolPath, 0o755); err != nil {
		t.Fatalf("Failed to create pool directory: %v", err)
	}

	// Create some test .deb files
	validPkg := filepath.ToSlash(filepath.Join("pool/main/f/foo/foo_1.0_amd64.deb"))

	// Create the files in the filesystem
	testData := []byte("test package data")
	fs.mu.Lock()
	fs.files["/mirror/pool/main/f/foo/foo_1.0_amd64.deb"] = &memFile{
		data:    testData,
		mode:    0o644,
		modTime: time.Now(),
	}
	fs.files["/mirror/pool/main/f/foo/foo_0.9_amd64.deb"] = &memFile{
		data:    testData,
		mode:    0o644,
		modTime: time.Now(),
	}
	fs.files["/mirror/pool/main/f/foo/foo_0.8_amd64.deb"] = &memFile{
		data:    testData,
		mode:    0o644,
		modTime: time.Now(),
	}
	fs.mu.Unlock()

	// Mark only one package as valid
	repo.validPackages[validPkg] = true

	// Run cleanup
	err := repo.cleanupOrphanedPackages()
	if err != nil {
		t.Fatalf("cleanupOrphanedPackages failed: %v", err)
	}

	// Verify valid package still exists
	if _, err := fs.Stat("/mirror/pool/main/f/foo/foo_1.0_amd64.deb"); err != nil {
		t.Error("valid package was incorrectly removed")
	}

	// Verify orphaned packages were removed
	if _, err := fs.Stat("/mirror/pool/main/f/foo/foo_0.9_amd64.deb"); err == nil {
		t.Error("orphaned package foo_0.9 was not removed")
	}

	if _, err := fs.Stat("/mirror/pool/main/f/foo/foo_0.8_amd64.deb"); err == nil {
		t.Error("orphaned package foo_0.8 was not removed")
	}
}

func TestCleanupOrphanedPackages_NoPool(t *testing.T) {
	fs := NewMemFileSystem()
	logger := &mockLogger{}
	downloader := &mockDownloader{}

	config := DittoConfig{
		DownloadPath: "/mirror",
		Logger:       logger,
		FileSystem:   fs,
		Downloader:   downloader,
	}

	repo := NewDittoRepo(config).(*dittoRepo)

	// Run cleanup when pool doesn't exist - should not error
	err := repo.cleanupOrphanedPackages()
	if err != nil {
		t.Fatalf("cleanupOrphanedPackages failed when pool doesn't exist: %v", err)
	}
}

func TestCleanupOrphanedPackages_IgnoresNonDebFiles(t *testing.T) {
	fs := NewMemFileSystem().(*MemFileSystem)
	logger := &mockLogger{}
	downloader := &mockDownloader{}

	config := DittoConfig{
		DownloadPath: "/mirror",
		Logger:       logger,
		FileSystem:   fs,
		Downloader:   downloader,
	}

	repo := NewDittoRepo(config).(*dittoRepo)

	// Set up pool directory with mixed file types
	poolPath := "/mirror/pool/main/f/foo"
	if err := fs.MkdirAll(poolPath, 0o755); err != nil {
		t.Fatalf("Failed to create pool directory: %v", err)
	}

	testData := []byte("test data")
	fs.mu.Lock()
	// Non-.deb files should be ignored even if not in validPackages
	fs.files["/mirror/pool/main/f/foo/README.txt"] = &memFile{
		data:    testData,
		mode:    0o644,
		modTime: time.Now(),
	}
	fs.files["/mirror/pool/main/f/foo/Packages"] = &memFile{
		data:    testData,
		mode:    0o644,
		modTime: time.Now(),
	}
	// This .deb is not in validPackages and should be removed
	fs.files["/mirror/pool/main/f/foo/orphan.deb"] = &memFile{
		data:    testData,
		mode:    0o644,
		modTime: time.Now(),
	}
	fs.mu.Unlock()

	// Run cleanup
	err := repo.cleanupOrphanedPackages()
	if err != nil {
		t.Fatalf("cleanupOrphanedPackages failed: %v", err)
	}

	// Verify non-.deb files still exist
	if _, err := fs.Stat("/mirror/pool/main/f/foo/README.txt"); err != nil {
		t.Error("README.txt was incorrectly removed")
	}

	if _, err := fs.Stat("/mirror/pool/main/f/foo/Packages"); err != nil {
		t.Error("Packages file was incorrectly removed")
	}

	// Verify orphaned .deb was removed
	if _, err := fs.Stat("/mirror/pool/main/f/foo/orphan.deb"); err == nil {
		t.Error("orphaned .deb was not removed")
	}
}

func TestCleanupOrphanedPackages_AllValid(t *testing.T) {
	fs := NewMemFileSystem().(*MemFileSystem)
	logger := &mockLogger{}
	downloader := &mockDownloader{}

	config := DittoConfig{
		DownloadPath: "/mirror",
		Logger:       logger,
		FileSystem:   fs,
		Downloader:   downloader,
	}

	repo := NewDittoRepo(config).(*dittoRepo)

	// Set up pool directory
	poolPath := "/mirror/pool/main/f/foo"
	if err := fs.MkdirAll(poolPath, 0o755); err != nil {
		t.Fatalf("Failed to create pool directory: %v", err)
	}

	pkg1 := filepath.ToSlash(filepath.Join("pool/main/f/foo/foo_1.0_amd64.deb"))
	pkg2 := filepath.ToSlash(filepath.Join("pool/main/f/foo/foo_2.0_amd64.deb"))

	testData := []byte("test package data")
	fs.mu.Lock()
	fs.files["/mirror/pool/main/f/foo/foo_1.0_amd64.deb"] = &memFile{
		data:    testData,
		mode:    0o644,
		modTime: time.Now(),
	}
	fs.files["/mirror/pool/main/f/foo/foo_2.0_amd64.deb"] = &memFile{
		data:    testData,
		mode:    0o644,
		modTime: time.Now(),
	}
	fs.mu.Unlock()

	// Mark both packages as valid
	repo.validPackages[pkg1] = true
	repo.validPackages[pkg2] = true

	// Run cleanup
	err := repo.cleanupOrphanedPackages()
	if err != nil {
		t.Fatalf("cleanupOrphanedPackages failed: %v", err)
	}

	// Verify both packages still exist
	if _, err := fs.Stat("/mirror/pool/main/f/foo/foo_1.0_amd64.deb"); err != nil {
		t.Error("valid package foo_1.0 was incorrectly removed")
	}

	if _, err := fs.Stat("/mirror/pool/main/f/foo/foo_2.0_amd64.deb"); err != nil {
		t.Error("valid package foo_2.0 was incorrectly removed")
	}

	// Check logger message
	found := false
	for _, msg := range logger.infoMsgs {
		if msg == "No orphaned packages found." {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected 'No orphaned packages found.' message in logs")
	}
}
