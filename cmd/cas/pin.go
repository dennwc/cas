package main

import (
	"fmt"

	"github.com/dennwc/cas"
	"github.com/dennwc/cas/types"
	"github.com/spf13/cobra"
)

func init() {
	cmd := &cobra.Command{
		Use:   "pin [name] ref",
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

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "list all pins and their references",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 0 {
				return fmt.Errorf("expected 0 arguments")
			}

			ctx := cmdCtx
			s, err := cas.Open(cas.OpenOptions{
				Dir: casDir, Create: false,
			})
			if err != nil {
				return err
			}

			it := s.IteratePins(ctx)
			defer it.Close()
			for it.Next() {
				fmt.Println(it.Name(), "=", it.Ref())
			}
			return it.Err()
		},
	}
	cmd.AddCommand(listCmd)

	getCmd := &cobra.Command{
		Use:   "get [name]",
		Short: "get the pinned reference",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) > 1 {
				return fmt.Errorf("expected 0 or 1 arguments")
			}
			name := cas.DefaultPin
			if len(args) != 0 {
				name = args[0]
			}

			ctx := cmdCtx
			s, err := cas.Open(cas.OpenOptions{
				Dir: casDir, Create: false,
			})
			if err != nil {
				return err
			}

			ref, err := s.GetPin(ctx, name)
			if err != nil {
				return err
			}
			fmt.Println(ref)
			return nil
		},
	}
	cmd.AddCommand(getCmd)

	delCmd := &cobra.Command{
		Use:   "del [name]",
		Short: "delete the pin",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) > 1 {
				return fmt.Errorf("expected 0 or 1 arguments")
			}
			name := cas.DefaultPin
			if len(args) != 0 {
				name = args[0]
			}

			ctx := cmdCtx
			s, err := cas.Open(cas.OpenOptions{
				Dir: casDir, Create: false,
			})
			if err != nil {
				return err
			}

			return s.DeletePin(ctx, name)
		},
	}
	cmd.AddCommand(delCmd)
}
