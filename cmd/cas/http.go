package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/dennwc/cas"
	"github.com/dennwc/cas/http"
	"github.com/dennwc/cas/schema"
	"github.com/dennwc/cas/types"
)

var (
	typeHTTPReq  = schema.MustTypeOf(&cashttp.Request{})
	typeHTTPSess = schema.MustTypeOf(&cashttp.Session{})
)

func init() {
	cmd := &cobra.Command{
		Use:   "http",
		Short: "commands related to HTTP protocol",
	}
	Root.AddCommand(cmd)

	listReqCmd := &cobra.Command{
		Use:   "req",
		Short: "list request schema blob(s)",
		RunE: casOpenCmd(func(ctx context.Context, st *cas.Storage, flags *pflag.FlagSet, args []string) error {
			u, _ := flags.GetString("url")

			it := st.IterateSchema(ctx, typeHTTPReq)
			defer it.Close()
			for it.Next() {
				obj, err := it.Decode()
				if err != nil {
					return err
				}
				r, ok := obj.(*cashttp.Request)
				if !ok {
					return fmt.Errorf("unsupported type: %T", obj)
				}
				if u != "" && !strings.HasPrefix(r.URL.String(), u) {
					continue
				}
				sr := it.SizedRef()
				fmt.Println(sr.Ref, r.Method, r.URL)
			}
			return it.Err()
		}),
	}
	listReqCmd.Flags().StringP("url", "u", "", "filter based on URL prefix")
	cmd.AddCommand(listReqCmd)

	listSessCmd := &cobra.Command{
		Use:   "sess",
		Short: "list recorded HTTP sessions",
		RunE: casOpenCmd(func(ctx context.Context, st *cas.Storage, flags *pflag.FlagSet, args []string) error {
			var reqRef, respRef types.Ref
			if s, _ := flags.GetString("req"); s != "" {
				var err error
				reqRef, err = types.ParseRef(s)
				if err != nil {
					return err
				}
			}
			if s, _ := flags.GetString("resp"); s != "" {
				var err error
				respRef, err = types.ParseRef(s)
				if err != nil {
					return err
				}
			}

			it := st.IterateSchema(ctx, typeHTTPSess)
			defer it.Close()
			for it.Next() {
				obj, err := it.Decode()
				if err != nil {
					return err
				}
				r, ok := obj.(*cashttp.Session)
				if !ok {
					return fmt.Errorf("unsupported type: %T", obj)
				}
				if !reqRef.Zero() && r.Request != reqRef {
					continue
				}
				if !respRef.Zero() && r.Response != respRef {
					continue
				}
				sr := it.SizedRef()
				fmt.Println(sr.Ref, r.Request, r.Response)
			}
			return it.Err()
		}),
	}
	listSessCmd.Flags().String("req", "", "filter based on a request ref")
	listSessCmd.Flags().String("resp", "", "filter based on a response ref")
	cmd.AddCommand(listSessCmd)
}
