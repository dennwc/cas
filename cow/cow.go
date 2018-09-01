// Package cow provides copy-on-write functionality for well-known filesystems.
package cow

import (
	"context"
	"errors"
	"os"
	"os/exec"
)

var ErrNotSupported = errors.New("copy-on-write is not supported")

// Clone makes a copy of file in dst, while reusing underlying FS blocks if possible.
func Clone(ctx context.Context, dst, src string) error {
	// TODO: use syscalls
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	cmd := exec.Command("cp", "--reflink=always", src, dst)
	cmd.Dir = wd
	if err := cmd.Run(); err != nil {
		return ErrNotSupported
	}
	return nil
}
