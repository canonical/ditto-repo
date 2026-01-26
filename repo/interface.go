package repo

import (
	"io"
	"os"
)

type DittoRepo interface {
	Mirror() error
}

// Logger is a simple logging interface
// It mimics the standard library log/slog methods.
type Logger interface {
	Debug(msg string, args ...any)
	Error(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
}

// FileSystem abstracts all filesystem operations needed for mirroring.
// This allows for testing and alternative storage backends.
type FileSystem interface {
	// ReadFile reads the entire file at the given path
	ReadFile(path string) ([]byte, error)

	// Stat returns file info for the given path
	Stat(path string) (os.FileInfo, error)

	// Open opens a file for reading
	Open(path string) (io.ReadCloser, error)

	// Create creates or truncates a file for writing
	Create(path string) (io.WriteCloser, error)

	// MkdirAll creates a directory and all necessary parents
	MkdirAll(path string, perm os.FileMode) error

	// Remove deletes a file or empty directory
	Remove(path string) error

	// Rename moves/renames a file or directory
	Rename(oldPath, newPath string) error

	// Link creates a hard link
	Link(oldPath, newPath string) error
}
