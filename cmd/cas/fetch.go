package main

import (
	"context"
	"fmt"
	"net/url"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/dennwc/cas"
	"github.com/dennwc/cas/types"
)

func init() {
	cmd := &cobra.Command{
		Use:   "fetch",
		Short: "store the URL or file in the content-addressable storage",
		RunE: casCreateCmd(func(ctx context.Context, s *cas.Storage, _ *pflag.FlagSet, args []string) error {
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
		}),
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
