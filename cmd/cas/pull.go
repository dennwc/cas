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
		Use:   "pull",
		Short: "store the URL or file in the content-addressable storage under a named pin",
		RunE: casCreateCmd(func(ctx context.Context, s *cas.Storage, flags *pflag.FlagSet, args []string) error {
			if len(args) == 0 || len(args) > 2 {
				return fmt.Errorf("expected 1 or 2 arguments")
			}
			addr := args[0]
			name := cas.DefaultPin
			if len(args) == 2 {
				name = args[0]
				addr = args[1]
			}

			conf := storeConfigFromFlags(flags)

			sr, err := s.StoreAddr(ctx, addr, conf)
			if err != nil {
				return err
			}
			if err := s.SetPin(ctx, name, sr.Ref); err != nil {
				return err
			}
			fmt.Println(name, "=", sr.Ref)
			return nil
		}),
	}
	registerStoreConfFlags(cmd.Flags())
	Root.AddCommand(cmd)
}
