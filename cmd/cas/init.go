package main

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/dennwc/cas"
)

const casDir = cas.DefaultDir

func init() {
	cmd := &cobra.Command{
		Use:   "init",
		Short: "init content-addressable storage in current directory",
		RunE: casCreateCmd(func(ctx context.Context, s *cas.Storage, _ *pflag.FlagSet, args []string) error {
			_ = s
			return nil
		}),
	}
	Root.AddCommand(cmd)
}
