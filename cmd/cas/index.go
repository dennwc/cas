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
		Use:   "index",
		Short: "index the URL or file to the content-addressable storage",
		RunE: casCreateCmd(func(ctx context.Context, s *cas.Storage, _ *pflag.FlagSet, args []string) error {
			var last error
			for _, arg := range args {
				sr, err := storeAddr(ctx, s, arg, true)
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
