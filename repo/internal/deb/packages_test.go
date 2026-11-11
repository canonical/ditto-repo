package deb

import (
	"strings"
	"testing"
)

func TestParsePackagesIndex(t *testing.T) {
	content := `Package: foo
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
	reader := strings.NewReader(content)

	packages, err := ParsePackagesIndex(reader)
	if err != nil {
		t.Fatalf("ParsePackagesIndex failed: %v", err)
	}

	if len(packages) != 2 {
		t.Fatalf("expected 2 packages, got %d", len(packages))
	}

	// Check first package.
	if packages[0].Path != "pool/main/f/foo/foo_1.0_amd64.deb" {
		t.Errorf("package 0 path: expected 'pool/main/f/foo/foo_1.0_amd64.deb', got '%s'", packages[0].Path)
	}
	if packages[0].SHA256 != "abc123def456abc123def456abc123def456abc123def456abc123def456abc1" {
		t.Errorf("package 0 SHA256: expected 'abc123...', got '%s'", packages[0].SHA256)
	}
	if packages[0].Size != 12345 {
		t.Errorf("package 0 Size: expected 12345, got %d", packages[0].Size)
	}

	// Check second package
	if packages[1].Path != "pool/main/b/bar/bar_2.0_amd64.deb" {
		t.Errorf("package 1 path: expected 'pool/main/b/bar/bar_2.0_amd64.deb', got '%s'", packages[1].Path)
	}
	if packages[1].SHA256 != "def456abc123def456abc123def456abc123def456abc123def456abc123def4" {
		t.Errorf("package 1 SHA256: expected 'def456...', got '%s'", packages[1].SHA256)
	}
	if packages[1].Size != 67890 {
		t.Errorf("package 1 Size: expected 67890, got %d", packages[1].Size)
	}
}

func TestParsePackages_EmptyFile(t *testing.T) {
	reader := strings.NewReader("")

	packages, err := ParsePackagesIndex(reader)
	if err != nil {
		t.Fatalf("ParsePackagesIndex failed: %v", err)
	}

	if len(packages) != 0 {
		t.Fatalf("expected 0 packages, got %d", len(packages))
	}
}

func TestParsePackages_IncompletePackage(t *testing.T) {
	// Package with missing SHA256.
	content := `Package: incomplete
Version: 1.0
Architecture: amd64
Filename: pool/main/i/incomplete/incomplete_1.0_amd64.deb
Size: 12345

`
	reader := strings.NewReader(content)

	packages, err := ParsePackagesIndex(reader)
	if err != nil {
		t.Fatalf("ParsePackagesIndex failed: %v", err)
	}

	// Package should be skipped because it's missing SHA256
	if len(packages) != 0 {
		t.Errorf("expected 0 packages (incomplete should be skipped), got %d", len(packages))
	}
}
