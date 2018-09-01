package main

import (
	"fmt"

	"github.com/dennwc/cas"
	"github.com/spf13/cobra"
)

func init() {
	cmd := &cobra.Command{
		Use:   "import",
		Short: "import a local file into CAS while reusing underlying storage",
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
				sr, err := s.ImportFilePath(ctx, arg)
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
