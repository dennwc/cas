package main

import (
	"fmt"

	"github.com/dennwc/cas"
	"github.com/spf13/cobra"
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
		RunE: func(cmd *cobra.Command, args []string) error {
			typs, _ := cmd.Flags().GetStringSlice("type")
			st, err := cas.Open(cas.OpenOptions{
				Dir: casDir, Create: false,
			})
			if err != nil {
				return err
			}
			it := st.IterateSchema(cmdCtx, typs...)
			defer it.Close()
			for it.Next() {
				fmt.Println(it.Ref(), it.Size(), it.Type())
			}
			return it.Err()
		},
	}
	listCmd.Flags().StringSliceP("type", "t", nil, "types to include")
	cmd.AddCommand(listCmd)
}
