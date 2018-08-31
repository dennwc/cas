package main

import (
	"log"

	"github.com/spf13/cobra"
)

var (
	Root = &cobra.Command{
		Use:   "cas [command]",
		Short: "Tools to manage Content Addressable Storage",
	}
)

func main() {
	if err := Root.Execute(); err != nil {
		log.Fatal(err)
	}
}
