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
		Use:     "schema",
		Aliases: []string{"sch"},
		Short:   "commands related to the CAS schema",
	}
	Root.AddCommand(cmd)

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "list schema blob(s) stored in CAS",
		RunE: casOpenCmd(func(ctx context.Context, st *cas.Storage, flags *pflag.FlagSet, args []string) error {
			typs, _ := flags.GetStringSlice("type")

			it := st.IterateSchema(cmdCtx, typs...)
			defer it.Close()
			for it.Next() {
				fmt.Println(it.Ref(), it.Size(), it.Type())
			}
			return it.Err()
		}),
	}
	listCmd.Flags().StringSliceP("type", "t", nil, "types to include")
	cmd.AddCommand(listCmd)
}
