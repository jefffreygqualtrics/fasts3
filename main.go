package main

import (
	"github.com/tuneinc/fasts3/cmd"
	"log"
)

var (
	version = "master"
)

func main() {
	log.SetFlags(log.Lshortfile)
	cmd.Version = version
	cmd.Execute()
}
