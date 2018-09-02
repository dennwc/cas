package main

import (
	"context"
	"fmt"

	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/dennwc/cas"
	"github.com/dennwc/cas/types"
)

func init() {
	cmd := &cobra.Command{
		Use:     "schema",
		Aliases: []string{"sch", "s", "sc"},
		Short:   "commands related to the CAS schema",
	}
	Root.AddCommand(cmd)

	listCmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"l", "ls"},
		Short:   "list schema blob(s) stored in CAS",
		RunE: casOpenCmd(func(ctx context.Context, st *cas.Storage, flags *pflag.FlagSet, args []string) error {
			typs, _ := flags.GetStringSlice("type")

			it := st.IterateSchema(ctx, typs...)
			defer it.Close()
			for it.Next() {
				fmt.Println(it.Ref(), it.Size(), it.Type())
			}
			return it.Err()
		}),
	}
	listCmd.Flags().StringSliceP("type", "t", nil, "types to include")
	cmd.AddCommand(listCmd)

	dataInCmd := &cobra.Command{
		Use:   "data-in ref",
		Short: "list all data blob(s) in a specified schema blob",
		RunE: casOpenCmd(func(ctx context.Context, st *cas.Storage, flags *pflag.FlagSet, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("expected one argument")
			}
			ref, err := types.ParseRef(args[0])
			if err != nil {
				return err
			}
			count, _ := flags.GetBool("count")
			limit, _ := flags.GetInt("limit")

			it := st.IterateDataBlobsIn(ctx, ref)
			defer it.Close()
			var (
				cnt  int
				size uint64
			)
			for (limit <= 0 || cnt < limit) && it.Next() {
				cnt++
				if count {
					// TODO: this can be optimized since dir blobs store aggregated count and size
					size += it.Size()
				} else {
					fmt.Println(it.Ref(), it.Size())
				}
			}
			if count {
				fmt.Println("blobs:", cnt, "size:", size, "("+humanize.Bytes(size)+")")
			}
			return it.Err()
		}),
	}
	dataInCmd.Flags().BoolP("count", "c", false, "count blobs and size without listing blobs")
	dataInCmd.Flags().IntP("limit", "n", 0, "limit the number of blobs")
	cmd.AddCommand(dataInCmd)
}
