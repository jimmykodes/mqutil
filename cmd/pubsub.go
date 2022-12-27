package cmd

import (
	"fmt"
	"os"

	"github.com/jimmykodes/mqutil/internal/pubsub"
)

func init() {
	ps, err := pubsub.Command()
	if err != nil {
		fmt.Println("error creating pubsub command:", err)
		os.Exit(1)
	}
	rootCmd.SubCommand(ps)
}
