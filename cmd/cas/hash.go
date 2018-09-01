package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/dennwc/cas"
	"github.com/dennwc/cas/types"
)

func init() {
	hashCmd := &cobra.Command{
		Use:     "hash",
		Aliases: []string{"sum"},
		Short:   "hash files",
		RunE: func(cmd *cobra.Command, args []string) error {
			force, _ := cmd.Flags().GetBool("force")
			ctx := cmdCtx
			ref := types.NewRef()
			h := ref.Hash()
			if len(args) == 0 {
				_, err := io.Copy(h, os.Stdin)
				if err != nil {
					return err
				}
				fmt.Println(ref.WithHash(h), "-")
				return nil
			}
			xerr := false
			for _, name := range args {
				err := filepath.Walk(name, func(name string, info os.FileInfo, err error) error {
					if err != nil {
						return err
					} else if info.IsDir() {
						return nil
					}
					if !force {
						if sr, err := cas.Stat(ctx, name); err == nil && !sr.Ref.Zero() {
							fmt.Println(sr.Ref, name, "(cached)")
							return nil
						}
					}
					f, err := os.Open(name)
					if err != nil {
						return err
					}
					defer f.Close()

					h.Reset()
					_, err = io.Copy(h, f)
					if err != nil {
						return err
					}
					ref = ref.WithHash(h)
					fmt.Println(ref, name)
					if err = cas.SaveRef(ctx, name, info, ref); err != nil && !xerr {
						log.Println(err)
						xerr = true
					}
					return nil
				})
				if err != nil {
					return err
				}
			}
			return nil
		},
	}
	hashCmd.Flags().BoolP("force", "f", false, "ignore refs cache")
	Root.AddCommand(hashCmd)
}
