package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/user"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/dennwc/cas"
	_ "github.com/dennwc/cas/storage/all"
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
			Dir: casDir,
		})
		if os.IsNotExist(err) {
			oerr := err
			var u *user.User
			u, err = user.Current()
			if err != nil {
				return err
			}
			dir := filepath.Join(u.HomeDir, casDir)
			st, err = cas.Open(cas.OpenOptions{
				Dir: dir,
			})
			if err != nil {
				return oerr // return original error
			}
			fmt.Fprintln(os.Stderr, "using global CAS:", dir)
		}
		if err != nil {
			return err
		}
		return fnc(cmdCtx, st, cmd.Flags(), args)
	}
}
