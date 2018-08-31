package main

import (
	"context"
	"io"
	"os"

	"github.com/dennwc/cas"
	"github.com/dennwc/cas/types"
	"github.com/spf13/cobra"
)

func init() {
	cmd := &cobra.Command{
		Use:     "blob",
		Aliases: []string{"blobs"},
		Short:   "commands related to the binary data in content-addressable storage",
	}
	Root.AddCommand(cmd)

	getCmd := &cobra.Command{
		Use:     "cat",
		Aliases: []string{"get", "dump"},
		Short:   "dump blob(s) content to stdout",
		RunE: func(cmd *cobra.Command, args []string) error {
			st, err := cas.Open(cas.OpenOptions{
				Dir: casDir, Create: false,
			})
			if err != nil {
				return err
			}
			var w io.Writer = os.Stdout
			for _, arg := range args {
				ref, err := types.ParseRef(arg)
				if err != nil {
					return err
				}
				if err := dumpFile(w, st, ref); err != nil {
					return err
				}
			}
			return nil
		},
	}
	cmd.AddCommand(getCmd)

}

func dumpFile(w io.Writer, st *cas.Storage, ref cas.Ref) error {
	rc, _, err := st.FetchBlob(context.Background(), ref)
	if err != nil {
		return err
	}
	defer rc.Close()
	_, err = io.Copy(w, rc)
	return err
}
