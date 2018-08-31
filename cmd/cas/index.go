package main

import (
	"fmt"

	"github.com/dennwc/cas"
	"github.com/spf13/cobra"
)

func init() {
	cmd := &cobra.Command{
		Use:   "index",
		Short: "index the URL or file to the content-addressable storage",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmdCtx
			s, err := cas.Open(cas.OpenOptions{
				Dir: casDir, Create: true,
			})
			if err != nil {
				return err
			}
			var last error
			for _, arg := range args {
				sr, err := storeAddr(ctx, s, arg, true)
				if err != nil {
					last = err
					fmt.Println(arg, err)
				} else {
					fmt.Println(sr.Ref, arg)
				}
			}
			return last
		},
	}
	Root.AddCommand(cmd)
}
