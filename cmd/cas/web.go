package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/dennwc/cas"
	"github.com/spf13/cobra"
)

func init() {
	webCmd := &cobra.Command{
		Use:   "web [command]",
		Short: "web-related commands",
	}
	Root.AddCommand(webCmd)

	wrefCmd := &cobra.Command{
		Use:   "ref [url]",
		Short: "make a ref for a web data",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("expected at least one url")
			}
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			for _, addr := range args {
				obj, err := cas.NewWebContentRef(addr)
				if err != nil {
					return err
				}
				err = enc.Encode(obj)
				if err != nil {
					return err
				}
			}
			return nil
		},
	}
	webCmd.AddCommand(wrefCmd)
}
