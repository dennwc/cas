// Package cow provides copy-on-write functionality for well-known filesystems.
package cow

import (
	"context"
	"os"
	"os/exec"
)

// Copy makes a copy of file in dst, while reusing underlying FS blocks if possible.
func Copy(ctx context.Context, dst, src string) error {
	// TODO: use syscalls
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	cmd := exec.Command("cp", "--reflink=auto", src, dst)
	cmd.Dir = wd
	return cmd.Run()
}
