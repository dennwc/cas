package main

import (
	"fmt"

	"github.com/dennwc/cas"
	"github.com/dennwc/cas/types"
	"github.com/spf13/cobra"
)

func init() {
	cmd := &cobra.Command{
		Use:   "pin",
		Short: "set a named pin pointing to a ref",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 || len(args) > 2 {
				return fmt.Errorf("expected 1 or 2 arguments")
			}
			sref := args[0]
			name := cas.DefaultPin
			if len(args) == 2 {
				name = args[0]
				sref = args[1]
			}
			ref, err := types.ParseRef(sref)
			if err != nil {
				return err
			}

			ctx := cmdCtx
			s, err := cas.Open(cas.OpenOptions{
				Dir: casDir, Create: true,
			})
			if err != nil {
				return err
			}

			if err := s.SetPin(ctx, name, ref); err != nil {
				return err
			}
			fmt.Println(name, "=", ref)
			return nil
		},
	}
	Root.AddCommand(cmd)
}
