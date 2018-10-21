package main

import (
	"context"
	"fmt"
	"log"

	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/dennwc/cas"
	"github.com/dennwc/cas/schema/filter"
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
			short, _ := flags.GetBool("short")
			typs, _ := flags.GetStringSlice("type")

			var f filter.Filter
			if expr, _ := flags.GetString("filter"); expr != "" {
				var err error
				f, err = filter.Compile(expr)
				if err != nil {
					return err
				}
			}

			var last error
			it := st.IterateSchema(ctx, typs...)
			defer it.Close()
			for it.Next() {
				sr := it.SchemaRef()
				if f != nil {
					obj, err := it.Decode()
					if err != nil {
						log.Println(err)
						last = err
						continue
					}
					ok, err := f.FilterObject(obj)
					if err != nil {
						return err
					} else if !ok {
						continue
					}
				}
				if short {
					fmt.Println(sr.Ref)
				} else {
					fmt.Println(sr.Ref, sr.Size, sr.Type)
				}
			}
			if it.Err() != nil {
				return it.Err()
			}
			return last
		}),
	}
	listCmd.Flags().StringSliceP("type", "t", nil, "types to include")
	listCmd.Flags().StringP("filter", "f", "", "expression to filter objects")
	listCmd.Flags().BoolP("short", "s", false, "only print refs")
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
				sr := it.SizedRef()
				if count {
					// TODO: this can be optimized since dir blobs store aggregated count and size
					size += sr.Size
				} else {
					fmt.Println(sr.Ref, sr.Size)
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

	reindexCmd := &cobra.Command{
		Use:   "reindex",
		Short: "list all data blob(s) in a specified schema blob",
		RunE: casOpenCmd(func(ctx context.Context, st *cas.Storage, flags *pflag.FlagSet, args []string) error {
			if len(args) != 0 {
				return fmt.Errorf("unexpected argument")
			}
			force, _ := flags.GetBool("force")
			return st.ReindexSchema(ctx, force)
		}),
	}
	reindexCmd.Flags().BoolP("force", "f", false, "force reindexing")
	cmd.AddCommand(reindexCmd)
}
