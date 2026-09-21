package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/m-ice/NewIM/server/buildinfo"
)

func main() {
	os.Exit(run(os.Stdout, os.Stderr, os.Args[1:], buildinfo.Read))
}

func run(stdout, stderr io.Writer, args []string, read func() (buildinfo.Info, error)) int {
	if len(args) == 1 && args[0] == "--help" {
		if _, err := fmt.Fprintln(stdout, "Usage: newim-buildinfo [--help]\nPrint embedded NewIM server build identity as JSON."); err != nil {
			fmt.Fprintln(stderr, "NIM_BUILD_OUTPUT_FAILED")
			return 1
		}
		return 0
	}
	if len(args) != 0 {
		fmt.Fprintln(stderr, "NIM_BUILD_INVALID_ARGUMENT")
		return 2
	}
	info, err := read()
	if err != nil {
		fmt.Fprintln(stderr, "NIM_BUILD_INVALID_METADATA")
		return 2
	}
	if err := json.NewEncoder(stdout).Encode(info); err != nil {
		fmt.Fprintln(stderr, "NIM_BUILD_OUTPUT_FAILED")
		return 1
	}
	return 0
}
