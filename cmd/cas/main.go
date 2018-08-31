package main

import (
	"context"
	"log"

	"github.com/spf13/cobra"
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
