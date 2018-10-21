//+build !linux

package local

import (
	"os"
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
