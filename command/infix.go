package main

import (
	"fmt"
	"os"

	"github.com/oktal/infix/rules"
)

const usage = `
Usage: infix [command] [arguments]

The commands are:

    rename-measurement  Rename a measurement
`

func printUsage() {
	fmt.Fprintln(os.Stdout, usage)
}

func main() {
	name, args := ParseCommandName(os.Args[1:])
	var rs []rules.Rule

	switch name {
	case "", "help":
		printUsage()
		os.Exit(0)
	case "rename-measurement":
		r, err := ParseRenameMeasurementCommand(args...)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		rs = append(rs, r)
	}
	rules := rules.NewChainingSet(rs)

	cmd := NewCommand(rules)
	if err := cmd.Run(os.Args[1:]...); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
