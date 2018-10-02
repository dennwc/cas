package main

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/dennwc/cas"
	"github.com/dennwc/cas/storage/http"
)

func init() {
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "serve CAS over HTTP",
		RunE: casOpenCmd(func(ctx context.Context, s *cas.Storage, flags *pflag.FlagSet, args []string) error {
			if len(args) != 0 {
				return fmt.Errorf("unexpected argument")
			}
			host, _ := flags.GetString("host")

			log.Println("listening on", host)
			srv := httpstor.NewServer(s, "/")
			return http.ListenAndServe(host, srv)
		}),
	}
	cmd.Flags().String("host", "localhost:9080", "host to listen on")
	Root.AddCommand(cmd)
}
