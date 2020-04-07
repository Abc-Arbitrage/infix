package main

import (
	"fmt"
	"os"
)

func main() {
	cmd := NewCommand()
	if err := cmd.Run(os.Args[1:]...); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
