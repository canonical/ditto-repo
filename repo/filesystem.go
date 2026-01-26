package repo

import (
	"io"
	"os"
)

// OsFileSystem is a FileSystem implementation that uses the real OS filesystem.
type OsFileSystem struct{}

func NewOsFileSystem() FileSystem {
	return &OsFileSystem{}
}

func (fs *OsFileSystem) ReadFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}

func (fs *OsFileSystem) Stat(path string) (os.FileInfo, error) {
	return os.Stat(path)
}

func (fs *OsFileSystem) Open(path string) (io.ReadCloser, error) {
	return os.Open(path)
}

func (fs *OsFileSystem) Create(path string) (io.WriteCloser, error) {
	return os.Create(path)
}

func (fs *OsFileSystem) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

func (fs *OsFileSystem) Remove(path string) error {
	return os.Remove(path)
}

func (fs *OsFileSystem) Rename(oldPath, newPath string) error {
	return os.Rename(oldPath, newPath)
}

func (fs *OsFileSystem) Link(oldPath, newPath string) error {
	return os.Link(oldPath, newPath)
}
