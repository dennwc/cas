//+build linux

package local

import (
	"os"
	"strconv"
	"time"
)

func fchtimes(f *os.File, atime, mtime time.Time) error {
	path := "/proc/self/fd/" + strconv.Itoa(int(f.Fd()))
	return os.Chtimes(path, atime, mtime)
}
