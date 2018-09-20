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
		Use:     "checkout [ref or pin] <dst>",
		Aliases: []string{"co", "restore"},
		Short:   "restore a pin or hash to a specified path",
		RunE: casOpenCmd(func(ctx context.Context, s *cas.Storage, _ *pflag.FlagSet, args []string) error {
			if len(args) != 1 && len(args) != 2 {
				return fmt.Errorf("expected 1 or 2 arguments")
			}
			path := args[0]
			name := cas.DefaultPin
			if len(args) == 2 {
				name = args[0]
				path = args[1]
			}

			ref, err := s.GetPinOrRef(ctx, name)
			if err != nil {
				return err
			}

			err = s.Checkout(ctx, ref, path)
			if err != nil {
				return err
			}
			fmt.Println(ref, "->", path)
			return nil
		}),
	}
	Root.AddCommand(cmd)
}
