package cmd

import (
	"github.com/jimmykodes/gommand"
)

var (
	rootCmd = &gommand.Command{
		Name:        "mqutil",
		Description: "A collection of convenience commands for interacting with message queues",
	}
)

func Execute() error {
	return rootCmd.Execute()
}
