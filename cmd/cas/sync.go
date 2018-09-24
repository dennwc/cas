package main

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/dennwc/cas"
	"github.com/dennwc/cas/types"
)

func init() {
	cmd := &cobra.Command{
		Use:   "sync",
		Short: "syncs one or more remote blobs",
		RunE: casOpenCmd(func(ctx context.Context, s *cas.Storage, flags *pflag.FlagSet, args []string) error {
			if len(args) == 0 {
				args = []string{cas.DefaultPin}
			}
			var last error
			for _, arg := range args {
				ref, err := s.GetPinOrRef(ctx, arg)
				if err != nil {
					last = err
					fmt.Println(arg, err)
					continue
				}
				nref, err := s.SyncBlob(ctx, ref)
				if err == nil && ref != nref && !types.IsRef(arg) {
					err = s.SetPin(ctx, arg, nref)
				}
				if err != nil {
					last = err
					fmt.Println(arg, err)
					continue
				}
				if ref != nref {
					fmt.Println(arg, "->", ref)
				} else {
					fmt.Println(arg, "->", ref, "(up-to-date)")
				}
			}
			return last
		}),
	}
	registerStoreConfFlags(cmd.Flags())
	Root.AddCommand(cmd)
}
