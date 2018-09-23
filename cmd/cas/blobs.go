package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/dennwc/cas"
)

func init() {
	cmd := &cobra.Command{
		Use:     "blob",
		Aliases: []string{"blobs", "b", "bl"},
		Short:   "commands related to the binary data in content-addressable storage",
	}
	Root.AddCommand(cmd)

	getCmd := &cobra.Command{
		Use:     "cat",
		Aliases: []string{"get", "dump"},
		Short:   "dump blob(s) content to stdout",
		RunE: casOpenCmd(func(ctx context.Context, st *cas.Storage, _ *pflag.FlagSet, args []string) error {
			var w io.Writer = os.Stdout
			for _, arg := range args {
				ref, err := st.GetPinOrRef(ctx, arg)
				if err != nil {
					return err
				}
				if err := dumpFile(ctx, w, st, ref); err != nil {
					return err
				}
			}
			return nil
		}),
	}
	cmd.AddCommand(getCmd)

	listCmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"l", "ls"},
		Short:   "list blob(s) stored in CAS",
		RunE: casOpenCmd(func(ctx context.Context, st *cas.Storage, _ *pflag.FlagSet, args []string) error {
			it := st.IterateBlobs(ctx)
			defer it.Close()
			for it.Next() {
				sr := it.SizedRef()
				fmt.Println(sr.Ref, sr.Size)
			}
			return it.Err()
		}),
	}
	cmd.AddCommand(listCmd)
}

func dumpFile(ctx context.Context, w io.Writer, st *cas.Storage, ref cas.Ref) error {
	rc, _, err := st.FetchBlob(ctx, ref)
	if err != nil {
		return err
	}
	defer rc.Close()
	_, err = io.Copy(w, rc)
	return err
}
