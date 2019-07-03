package main

import (
	"log"

	"github.com/metaverse/fasts3/cmd"
)

var (
	version = "master"
)

func main() {
	log.SetFlags(log.Lshortfile)
	cmd.Version = version
	cmd.Execute()
}
