package main

import (
	"context"
	"fmt"
	"net/url"

	"github.com/dennwc/cas"
	"github.com/spf13/cobra"
)

func init() {
	cmd := &cobra.Command{
		Use:   "pull",
		Short: "store the URL or file in the content-addressable storage",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			s, err := cas.Open(cas.OpenOptions{
				Dir: casDir, Create: true,
			})
			if err != nil {
				return err
			}
			var last error
			for _, arg := range args {
				var (
					sr cas.SizedRef
				)
				u, err := url.Parse(arg)
				if err != nil {
					last = err
					fmt.Println(arg, err)
					continue
				}
				if u.Scheme != "" {
					sr, err = s.StoreURLContent(ctx, arg)
				} else {
					sr, err = s.StoreFilePath(ctx, arg)
				}
				if err != nil {
					last = err
					fmt.Println(arg, err)
				} else {
					fmt.Println(sr.Ref, sr.Size, arg)
				}
			}
			return last
		},
	}
	Root.AddCommand(cmd)
}
