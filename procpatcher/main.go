package main

import (
	"fmt"
	"os"
	"path/filepath"

	"procpatcher/patcher"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "Usage: procpatcher <input.sql> [output.sql]")
		os.Exit(1)
	}

	inputPath := os.Args[1]
	var outputPath string
	if len(os.Args) >= 3 {
		outputPath = os.Args[2]
	}

	// Read input file
	inputSQL, err := os.ReadFile(inputPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input file: %v\n", err)
		os.Exit(1)
	}

	// Patch the SQL
	result, err := patcher.Patch(string(inputSQL))
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

		// Generate helper.sql in the same directory as output
		helperPath := filepath.Join(filepath.Dir(outputPath), "__ultraverse__helper.sql")
		err = os.WriteFile(helperPath, []byte(generateHelperSQL()), 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error writing helper file: %v\n", err)
			os.Exit(1)
		}
	} else {
		fmt.Print(result.PatchedSQL)

		// Generate helper.sql in CWD
		helperPath := "__ultraverse__helper.sql"
		err = os.WriteFile(helperPath, []byte(generateHelperSQL()), 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error writing helper file: %v\n", err)
			os.Exit(1)
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
