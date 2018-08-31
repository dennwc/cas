package main

import (
	"fmt"

	"github.com/dennwc/cas"
	"github.com/spf13/cobra"
)

func init() {
	cmd := &cobra.Command{
		Use:   "pull",
		Short: "store the URL or file in the content-addressable storage under a named pin",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 || len(args) > 2 {
				return fmt.Errorf("expected 1 or 2 arguments")
			}
			addr := args[0]
			name := cas.DefaultPin
			if len(args) == 2 {
				name = args[0]
				addr = args[1]
			}
			ctx := cmdCtx
			s, err := cas.Open(cas.OpenOptions{
				Dir: casDir, Create: true,
			})
			if err != nil {
				return err
			}

			sr, err := storeAddr(ctx, s, addr, false)
			if err != nil {
				return err
			}
			if err := s.SetPin(ctx, name, sr.Ref); err != nil {
				return err
			}
			fmt.Println(name, "=", sr.Ref)
			return nil
		},
	}
	Root.AddCommand(cmd)
}
