package main

import (
	"fmt"
	"os"
	"path/filepath"

	"procpatcher/patcher"
)

func main() {
	args := os.Args[1:]
	mode := "patch"
	if len(args) > 0 && (args[0] == "--depatch" || args[0] == "-d") {
		mode = "depatch"
		args = args[1:]
	} else if len(args) > 0 && (args[0] == "--repatch" || args[0] == "-r") {
		mode = "repatch"
		args = args[1:]
	}

	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Usage: procpatcher [--depatch|-d] [--repatch|-r] <input.sql> [output.sql]")
		os.Exit(1)
	}

	inputPath := args[0]
	var outputPath string
	if len(args) >= 2 {
		outputPath = args[1]
	}

	// Read input file
	inputSQL, err := os.ReadFile(inputPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input file: %v\n", err)
		os.Exit(1)
	}

	var result *patcher.PatchResult
	if mode == "depatch" {
		result, err = patcher.Depatch(string(inputSQL))
	} else if mode == "repatch" {
		result, err = patcher.Repatch(string(inputSQL))
	} else {
		result, err = patcher.Patch(string(inputSQL))
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error patching SQL: %v\n", err)
		os.Exit(1)
	}

	// Write output
	if outputPath != "" {
		err = os.WriteFile(outputPath, []byte(result.PatchedSQL), 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error writing output file: %v\n", err)
			os.Exit(1)
		}

		if mode != "depatch" {
			// Generate helper.sql in the same directory as output
			helperPath := filepath.Join(filepath.Dir(outputPath), "__ultraverse__helper.sql")
			err = os.WriteFile(helperPath, []byte(generateHelperSQL()), 0644)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error writing helper file: %v\n", err)
				os.Exit(1)
			}
		}
	} else {
		fmt.Print(result.PatchedSQL)

		if mode != "depatch" {
			// Generate helper.sql in CWD
			helperPath := "__ultraverse__helper.sql"
			err = os.WriteFile(helperPath, []byte(generateHelperSQL()), 0644)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error writing helper file: %v\n", err)
				os.Exit(1)
			}
		}
	}

	// Print warnings to stderr
	for _, warn := range result.Warnings {
		fmt.Fprintln(os.Stderr, warn)
	}
}

func generateHelperSQL() string {
	return `-- Ultraverse Procedure Hint Table
-- This BLACKHOLE table captures procedure call information for retroactive operation tracking

CREATE TABLE IF NOT EXISTS __ULTRAVERSE_PROCEDURE_HINT (
    callid BIGINT UNSIGNED NOT NULL,
    procname VARCHAR(255) NOT NULL,
    args VARCHAR(4096),
    vars VARCHAR(4096),
    PRIMARY KEY (callid)
) ENGINE = BLACKHOLE;
`
}
