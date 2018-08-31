package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/dennwc/cas/types"
	"github.com/spf13/cobra"
)

func init() {
	hashCmd := &cobra.Command{
		Use:     "hash",
		Aliases: []string{"sum"},
		Short:   "hash files",
		RunE: func(cmd *cobra.Command, args []string) error {
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
			for _, name := range args {
				err := filepath.Walk(name, func(name string, info os.FileInfo, err error) error {
					if err != nil {
						return err
					} else if info.IsDir() {
						return nil
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
					fmt.Println(ref.WithHash(h), name)
					return nil
				})
				if err != nil {
					return err
				}
			}
			return nil
		},
	}
	Root.AddCommand(hashCmd)
}
