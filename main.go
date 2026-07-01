package main

import (
	"errors"
	"fmt"
	"log"
	"os"
)

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		var ue *usageError
		if errors.As(err, &ue) {
			fmt.Fprintf(os.Stderr, "gocachez: %v\n\n", ue.err)
			_ = writeHelp(os.Stderr, ue.mode)
			os.Exit(2)
		}
		log.Fatal(err)
	}
}
