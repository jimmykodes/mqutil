package main

import (
	"os"

	"github.com/jimmykodes/mqutil/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
