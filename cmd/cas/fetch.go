package main

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/dennwc/cas"
)

func registerStoreConfFlags(flags *pflag.FlagSet) {
	flags.BoolP("index", "i", false, "index only; do not store content blobs")
	flags.Bool("split", false, "split content blobs")
	flags.Uint64("max", 0, "max size of chunks while splitting")
}

func storeConfigFromFlags(flags *pflag.FlagSet) *cas.StoreConfig {
	conf := &cas.StoreConfig{}
	conf.IndexOnly, _ = flags.GetBool("index")
	if split, _ := flags.GetBool("split"); split {
		conf.Split = &cas.SplitConfig{}
		conf.Split.Max, _ = flags.GetUint64("max")
	}
	return conf
}

func init() {
	cmd := &cobra.Command{
		Use:   "fetch",
		Short: "store the URL or file in the content-addressable storage",
		RunE: casCreateCmd(func(ctx context.Context, s *cas.Storage, flags *pflag.FlagSet, args []string) error {
			conf := storeConfigFromFlags(flags)

			var last error
			for _, arg := range args {
				sr, err := s.StoreAddr(ctx, arg, conf)
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
	registerStoreConfFlags(cmd.Flags())
	Root.AddCommand(cmd)
}
