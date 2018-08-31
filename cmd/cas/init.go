package main

import (
	"github.com/dennwc/cas"
	"github.com/spf13/cobra"
)

const casDir = cas.DefaultDir

func init() {
	cmd := &cobra.Command{
		Use:   "init",
		Short: "init content-addressable storage in current directory",
		RunE: func(cmd *cobra.Command, args []string) error {
			st, err := cas.Open(cas.OpenOptions{
				Dir: casDir, Create: true,
			})
			if err != nil {
				return err
			}
			_ = st
			return nil
		},
	}
	Root.AddCommand(cmd)
}
