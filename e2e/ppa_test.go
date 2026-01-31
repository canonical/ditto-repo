package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

const binaryName = "ditto"

var binaryPath string

// Compiles the binary into a temporary directory once before running tests. Any tests can re-use
// the same binary.
// Sets up an apt sandbox environment
func TestMain(m *testing.M) {
	tmpDir, err := os.MkdirTemp("", "e2e-test-build")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create temp dir: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tmpDir)

	binaryPath, err = compileBinary(tmpDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}

	exitCode := m.Run()

	os.Exit(exitCode)
}

// TestPPAMirror tests mirroring a PPA repository and validates that a specific package file is downloaded.
func TestPPAMirror(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "e2e-ppa-mirror")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Configure ditto
	repoURL := "https://ppa.launchpadcontent.net/mitchburton/snap-http/ubuntu/"
	dist := "noble"
	components := "main"
	archs := "amd64"

	cmd := exec.Command(
		binaryPath,
		"--repo-url", repoURL,
		"--dist", dist,
		"--components", components,
		"--archs", archs,
		"--download-path", tmpDir,
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to execute mirror command: %v\nOutput:\n%s", err, string(output))
	}

	// Validate that the expected package file exists
	expectedPackagePath := filepath.Join(tmpDir, "pool", "main", "s", "snap-http", "python3-snap-http_1.4.0-0ubuntu0_all.deb")
	if _, err := os.Stat(expectedPackagePath); os.IsNotExist(err) {
		t.Errorf("Expected package file not found: %s", expectedPackagePath)
		t.Logf("Mirror output:\n%s", string(output))
	} else if err != nil {
		t.Fatalf("Failed to stat package file: %v", err)
	}

	t.Logf("Successfully mirrored PPA and validated package file exists")
}

// Compiles the ditto binary. We assume "../cmd/main.go" is the location of main package.
func compileBinary(outputDir string) (string, error) {
	binaryPath = filepath.Join(outputDir, binaryName)

	buildCmd := exec.Command("go", "build", "-o", binaryPath, "../cmd/main.go")
	if output, err := buildCmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("failed to build binary: %v\nOutput:\n%s", err, string(output))
	}

	return binaryPath, nil
}
