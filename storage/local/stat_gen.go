//+build !linux

package local

import (
	"os"
	"time"
)

func fchtimes(f *os.File, atime, mtime time.Time) error {
	return os.Chtimes(f.Name(), atime, mtime)
}
