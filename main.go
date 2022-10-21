package main

import (
	"os"

	"github.com/txtweet/velov_beat/cmd"

	_ "github.com/txtweet/velov_beat/include"
)

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
