package main

import (
	"context"
	"fmt"
	"net/url"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/dennwc/cas"
	"github.com/dennwc/cas/config"
	"github.com/dennwc/cas/storage"
	"github.com/dennwc/cas/storage/http"
	"github.com/dennwc/cas/storage/local"
)

const casDir = cas.DefaultDir

type casInitE func(ctx context.Context, flags *pflag.FlagSet, args []string) (storage.Config, error)

func casInitCmd(fnc casInitE) cobraRunE {
	return func(cmd *cobra.Command, args []string) error {
		sconf, err := fnc(cmdCtx, cmd.Flags(), args)
		if err != nil {
			return err
		}
		return cas.Init(casDir, &config.Config{Storage: sconf})
	}
}

func init() {
	cmd := &cobra.Command{
		Use:   "init",
		Short: "init content-addressable storage in current directory",
		RunE: casInitCmd(func(ctx context.Context, _ *pflag.FlagSet, args []string) (storage.Config, error) {
			return &local.Config{Dir: "."}, nil
		}),
	}
	Root.AddCommand(cmd)

	initHTTPCmd := &cobra.Command{
		Use:     "http",
		Aliases: []string{"remote", "client"},
		Short:   "init a client to a remote content-addressable storage",
		RunE: casInitCmd(func(ctx context.Context, _ *pflag.FlagSet, args []string) (storage.Config, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("expected a URL of the server")
			}
			addr := args[0]
			_, err := url.Parse(addr)
			if err != nil {
				return nil, err
			}
			return &httpstor.Config{URL: addr}, nil
		}),
	}
	cmd.AddCommand(initHTTPCmd)
}
