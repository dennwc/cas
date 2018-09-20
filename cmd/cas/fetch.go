package main

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/dennwc/cas"
)

func init() {
	cmd := &cobra.Command{
		Use:   "fetch",
		Short: "store the URL or file in the content-addressable storage",
		RunE: casCreateCmd(func(ctx context.Context, s *cas.Storage, _ *pflag.FlagSet, args []string) error {
			var last error
			for _, arg := range args {
				sr, err := s.StoreAddr(ctx, arg, false)
				if err != nil {
					last = err
					fmt.Println(arg, err)
				} else {
					fmt.Println(sr.Ref, arg)
				}
			}
			return last
		}),
	}
	Root.AddCommand(cmd)
}
