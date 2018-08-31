package main

import (
	"context"
	"fmt"
	"net/url"

	"github.com/dennwc/cas/types"

	"github.com/dennwc/cas"
	"github.com/spf13/cobra"
)

func init() {
	cmd := &cobra.Command{
		Use:   "fetch",
		Short: "store the URL or file in the content-addressable storage",
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
				sr, err := storeAddr(ctx, s, arg, false)
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

func storeAddr(ctx context.Context, s *cas.Storage, addr string, index bool) (types.SizedRef, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return types.SizedRef{}, err
	}
	if index {
		if u.Scheme != "" {
			return s.IndexURLContent(ctx, addr)
		}
		return s.IndexFilePath(ctx, addr)
	}
	if u.Scheme != "" {
		return s.StoreURLContent(ctx, addr)
	}
	return s.StoreFilePath(ctx, addr)
}
