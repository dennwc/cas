package main

import (
	"context"
	"log"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/dennwc/cas"
)

var (
	Root = &cobra.Command{
		Use:   "cas [command]",
		Short: "Content Addressable Storage",
	}
)

var cmdCtx = context.Background()

func main() {
	if err := Root.Execute(); err != nil {
		log.Fatal(err)
	}
}

type cobraRunE func(cmd *cobra.Command, args []string) error
type casRunE func(ctx context.Context, st *cas.Storage, flags *pflag.FlagSet, args []string) error

func casOpenCmd(fnc casRunE) cobraRunE {
	return func(cmd *cobra.Command, args []string) error {
		st, err := cas.Open(cas.OpenOptions{
			Dir: casDir, Create: false,
		})
		if err != nil {
			return err
		}
		return fnc(cmdCtx, st, cmd.Flags(), args)
	}
}

func casCreateCmd(fnc casRunE) cobraRunE {
	return func(cmd *cobra.Command, args []string) error {
		st, err := cas.Open(cas.OpenOptions{
			Dir: casDir, Create: true,
		})
		if err != nil {
			return err
		}
		return fnc(cmdCtx, st, cmd.Flags(), args)
	}
}
