package repo

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// MemFileSystem is an in-memory implementation of FileSystem for testing.
type MemFileSystem struct {
	mu    sync.RWMutex
	files map[string]*memFile
}

// memFile represents a file in memory.
type memFile struct {
	data    []byte
	mode    os.FileMode
	modTime time.Time
	isDir   bool
}

func NewMemFileSystem() FileSystem {
	return &MemFileSystem{
		files: make(map[string]*memFile),
	}
}

// normalizePath normalizes a path for consistent storage.
func normalizePath(path string) string {
	path = filepath.Clean(path)
	if path == "." {
		return "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return path
}

// ReadFile reads the entire file at the given path.
func (fs *MemFileSystem) ReadFile(path string) ([]byte, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	path = normalizePath(path)
	file, exists := fs.files[path]
	if !exists {
		return nil, &os.PathError{Op: "read", Path: path, Err: os.ErrNotExist}
	}
	if file.isDir {
		return nil, &os.PathError{Op: "read", Path: path, Err: fmt.Errorf("is a directory")}
	}

	// Return a copy to prevent external modification
	data := make([]byte, len(file.data))
	copy(data, file.data)
	return data, nil
}

// Stat returns file info for the given path.
func (fs *MemFileSystem) Stat(path string) (os.FileInfo, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	path = normalizePath(path)
	file, exists := fs.files[path]
	if !exists {
		return nil, &os.PathError{Op: "stat", Path: path, Err: os.ErrNotExist}
	}

	return &memFileInfo{
		name:    filepath.Base(path),
		size:    int64(len(file.data)),
		mode:    file.mode,
		modTime: file.modTime,
		isDir:   file.isDir,
	}, nil
}

// Open opens a file for reading.
func (fs *MemFileSystem) Open(path string) (io.ReadCloser, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	path = normalizePath(path)
	file, exists := fs.files[path]
	if !exists {
		return nil, &os.PathError{Op: "open", Path: path, Err: os.ErrNotExist}
	}
	if file.isDir {
		return nil, &os.PathError{Op: "open", Path: path, Err: fmt.Errorf("is a directory")}
	}

	// Return a reader with a copy of the data
	data := make([]byte, len(file.data))
	copy(data, file.data)
	return io.NopCloser(bytes.NewReader(data)), nil
}

// Create creates or truncates a file for writing.
func (fs *MemFileSystem) Create(path string) (io.WriteCloser, error) {
	path = normalizePath(path)

	// Ensure parent directory exists
	dir := filepath.Dir(path)
	if dir != "/" && dir != "." {
		fs.mu.RLock()
		if _, exists := fs.files[dir]; !exists {
			fs.mu.RUnlock()
			return nil, &os.PathError{Op: "create", Path: path, Err: os.ErrNotExist}
		}
		fs.mu.RUnlock()
	}

	return &memFileWriter{
		fs:   fs,
		path: path,
		buf:  new(bytes.Buffer),
	}, nil
}

// MkdirAll creates a directory and all necessary parents.
func (fs *MemFileSystem) MkdirAll(path string, perm os.FileMode) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	path = normalizePath(path)
	if path == "/" {
		return nil // Root always exists
	}

	// Create all parent directories
	parts := strings.Split(strings.Trim(path, "/"), "/")
	current := ""
	for _, part := range parts {
		current = current + "/" + part
		if _, exists := fs.files[current]; !exists {
			fs.files[current] = &memFile{
				mode:    perm | os.ModeDir,
				modTime: time.Now(),
				isDir:   true,
			}
		}
	}

	return nil
}

// Remove deletes a file or empty directory.
func (fs *MemFileSystem) Remove(path string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	path = normalizePath(path)
	if _, exists := fs.files[path]; !exists {
		// os.Remove doesn't return an error if file doesn't exist in some cases
		// But we'll be consistent and return nil
		return nil
	}

	delete(fs.files, path)
	return nil
}

// Rename moves/renames a file or directory.
func (fs *MemFileSystem) Rename(oldPath, newPath string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	oldPath = normalizePath(oldPath)
	newPath = normalizePath(newPath)

	file, exists := fs.files[oldPath]
	if !exists {
		return &os.PathError{Op: "rename", Path: oldPath, Err: os.ErrNotExist}
	}

	// Move the file
	fs.files[newPath] = file
	delete(fs.files, oldPath)

	return nil
}

// Link creates a hard link.
func (fs *MemFileSystem) Link(oldPath, newPath string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	oldPath = normalizePath(oldPath)
	newPath = normalizePath(newPath)

	file, exists := fs.files[oldPath]
	if !exists {
		return &os.PathError{Op: "link", Path: oldPath, Err: os.ErrNotExist}
	}
	if file.isDir {
		return &os.PathError{Op: "link", Path: oldPath, Err: fmt.Errorf("is a directory")}
	}

	// Create a new reference to the same data (hard link simulation)
	// In a real implementation, we might track link counts, but for testing this is sufficient
	fs.files[newPath] = &memFile{
		data:    file.data, // Share the same underlying data
		mode:    file.mode,
		modTime: file.modTime,
		isDir:   false,
	}

	return nil
}

// memFileWriter is an io.WriteCloser for writing to an in-memory file.
type memFileWriter struct {
	fs   *MemFileSystem
	path string
	buf  *bytes.Buffer
}

// Write writes data to the buffer.
func (w *memFileWriter) Write(p []byte) (n int, err error) {
	return w.buf.Write(p)
}

// Close finalizes the write and stores the file in the filesystem.
func (w *memFileWriter) Close() error {
	w.fs.mu.Lock()
	defer w.fs.mu.Unlock()

	w.fs.files[w.path] = &memFile{
		data:    w.buf.Bytes(),
		mode:    0o644,
		modTime: time.Now(),
		isDir:   false,
	}

	return nil
}

// memFileInfo implements os.FileInfo for in-memory files.
type memFileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
}

func (fi *memFileInfo) Name() string       { return fi.name }
func (fi *memFileInfo) Size() int64        { return fi.size }
func (fi *memFileInfo) Mode() os.FileMode  { return fi.mode }
func (fi *memFileInfo) ModTime() time.Time { return fi.modTime }
func (fi *memFileInfo) IsDir() bool        { return fi.isDir }
func (fi *memFileInfo) Sys() interface{}   { return nil }
