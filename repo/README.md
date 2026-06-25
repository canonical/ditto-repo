# ditto-repo Library

This package provides functionality for mirroring repositories and can be used as a library in your Go projects.

## Using ditto-repo as a Library

The `repo` package exports several interfaces that allow you to customize the behavior of the repository mirroring system:

- **`Logger`**: For logging debug, info, warning, and error messages
- **`FileSystem`**: For abstracting filesystem operations
- **`Downloader`**: For handling HTTP downloads

## Implementing Custom Interfaces

### Custom Logger

You can provide your own logging implementation by satisfying the `Logger` interface. The interface is currently a subset of common logging methods found in Go's `slog` package:

```go
type Logger interface {
    Debug(msg string, args ...any)
    Error(msg string, args ...any)
    Info(msg string, args ...any)
    Warn(msg string, args ...any)
}
```

### Custom FileSystem

Implement the `FileSystem` interface to use alternative storage backends:

```go
type FileSystem interface {
    ReadFile(path string) ([]byte, error)
    Stat(path string) (os.FileInfo, error)
    Open(path string) (io.ReadCloser, error)
    Create(path string) (io.WriteCloser, error)
    MkdirAll(path string, perm os.FileMode) error
    Remove(path string) error
    Rename(oldPath, newPath string) error
    Link(oldPath, newPath string) error
}
```

### Custom Downloader

Implement the `Downloader` interface to customize download behavior:

```go
type Downloader interface {
    // DownloadFile fetches a URL to a local path with atomic writing and checksum verification.
    // Returns the calculated SHA256 hash on success.
    // If expectedSHA256 is non-empty, the download will be verified against it.
    DownloadFile(urlStr string, destPath string, expectedSHA256 string) (string, error)
}
```

### Injecting your implementations

You can inject your custom implementations into the `repo` package by including them in your `DittoConfig` struct:

```go
import (
    "context"
    "log"
    "time"
    
    "github.com/canonical/ditto-repo/repo"
)

myConfig := repo.DittoConfig{
    RepoURLs:     []string{"http://archive.ubuntu.com/ubuntu"},
    Dists:        []string{"noble"},
    Components:   []string{"main"},
    Archs:        []string{"amd64"},
    Languages:    []string{"en"},
    DownloadPath: "./mirror",
    Workers:      5,
    AllowMissingIndices: false, // set true to warn instead of fail on missing Packages index files
    // Optional: aggregate several mirrors that publish an identical Release file.
    // All mirrors are validated for Release consistency before downloading, and
    // files are fetched with failover across the list.
    // RepoURLs: []string{
    //     "http://archive.ubuntu.com/ubuntu",
    //     "http://ports.ubuntu.com",
    // },
    // ArchURLs: map[string]string{ // optional per-arch "try first" hint
    //     "arm64": "http://ports.ubuntu.com",
    // },
    // RepoURL is the deprecated single-URL form; prefer RepoURLs.
    // Optional custom implementations:
    // Logger:     myLoggerImplementation,
    // FileSystem: myFileSystemImplementation,
    // Downloader: myDownloaderImplementation,
}

dittoRepo := repo.NewDittoRepo(myConfig)

// Create a context with timeout (optional)
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
defer cancel()

// Start mirroring and receive progress updates
progressChan, errChan := dittoRepo.MirrorWithErrors(ctx)
for update := range progressChan {
    log.Printf("Progress: %d/%d packages downloaded (Current: %s)",
        update.PackagesDownloaded, update.TotalPackages, update.CurrentFile)
}

// After the progress channel is drained, the error channel reports the
// terminal result: nil on success, or an aggregated error on failure.
if err := <-errChan; err != nil {
    log.Fatalf("Mirror failed: %v", err)
}

log.Println("Mirror complete!")
```

> The original `Mirror(ctx)` method is still available and returns only the progress
> channel; use it when you don't need to detect failures programmatically.

## Concurrent Operation

The `Mirror` method operates concurrently and supports:

- **Context-based cancellation**: Pass a `context.Context` to enable timeout or cancellation of the mirroring operation
- **Progress monitoring**: Receive real-time progress updates through a channel containing `ProgressUpdate` events
- **Error reporting**: `MirrorWithErrors` returns a second channel that yields the terminal result once mirroring finishes—`nil` on success, or an aggregated error describing the failures (`Mirror` reports failures through the logger only)
- **Graceful shutdown**: When the context is cancelled, workers stop processing new downloads and the channel is closed

### ProgressUpdate Structure

```go
type ProgressUpdate struct {
    PackagesDownloaded int    // Number of packages downloaded so far
    TotalPackages      int    // Total number of packages to download
    CurrentFile        string // Name of the file currently being processed
}
```