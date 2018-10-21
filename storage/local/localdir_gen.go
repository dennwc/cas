//+build !linux

package local

import (
	"os"
	"path/filepath"
)

type storageImpl struct{}

func (s *Storage) init() error {
	return nil
}

func (s *Storage) close() error {
	return nil
}

func (s *Storage) tmpFile(rw bool) (tempFile, error) {
	return s.tmpFileGen()
}

const cloneSupported = false

func cloneFile(dst, src *os.File) error {
	return errCantClone
}

func linkFile(dir *os.File, name string, file *os.File) error {
	return os.Link(file.Name(), filepath.Join(dir.Name(), name))
}
