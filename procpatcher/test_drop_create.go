//go:build ignore

package main

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/parser"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
)

type TestCase struct {
	Name string
	SQL  string
}

func main() {
	cases := []TestCase{
		{
			Name: "DROP FUNCTION IF EXISTS",
			SQL:  `DROP FUNCTION IF EXISTS test_func;`,
		},
		{
			Name: "DROP PROCEDURE IF EXISTS",
			SQL:  `DROP PROCEDURE IF EXISTS test_proc;`,
		},
		{
			Name: "CREATE FUNCTION basic",
			SQL: `CREATE FUNCTION test_func(x INT) RETURNS INT
BEGIN
    RETURN x * 2;
END;`,
		},
		{
			Name: "CREATE FUNCTION with DETERMINISTIC",
			SQL: `CREATE FUNCTION test_func(x INT) RETURNS INT DETERMINISTIC
BEGIN
    RETURN x * 2;
END;`,
		},
		{
			Name: "CREATE FUNCTION with NO SQL",
			SQL: `CREATE FUNCTION test_func(x INT) RETURNS INT NO SQL
BEGIN
    RETURN x * 2;
END;`,
		},
		{
			Name: "CREATE FUNCTION with READS SQL DATA",
			SQL: `CREATE FUNCTION get_name(p_id INT) RETURNS VARCHAR(100) READS SQL DATA
BEGIN
    DECLARE v_name VARCHAR(100);
    SELECT name INTO v_name FROM users WHERE id = p_id;
    RETURN v_name;
END;`,
		},
		{
			Name: "CREATE FUNCTION RETURNS FLOAT",
			SQL: `CREATE FUNCTION RandomNumber(minval INT, maxval INT) RETURNS FLOAT
BEGIN
    RETURN FLOOR(RAND()*(maxval - minval + 1)) + minval;
END;`,
		},
		{
			Name: "CREATE FUNCTION RETURNS DECIMAL",
			SQL: `CREATE FUNCTION calc_tax(amount DECIMAL(10,2)) RETURNS DECIMAL(10,2)
BEGIN
    RETURN amount * 0.1;
END;`,
		},
	}

	p := parser.New()

	fmt.Println("=== tidb/parser DROP/CREATE FUNCTION Support Test ===")
	fmt.Println()

	supported := 0
	unsupported := 0

	for _, tc := range cases {
		_, _, err := p.Parse(tc.SQL, "", "")
		if err != nil {
			fmt.Printf("❌ %s\n", tc.Name)
			fmt.Printf("   Error: %v\n\n", err)
			unsupported++
		} else {
			fmt.Printf("✅ %s\n\n", tc.Name)
			supported++
		}
	}

	fmt.Println("=== Summary ===")
	fmt.Printf("Supported: %d / %d\n", supported, len(cases))
	fmt.Printf("Unsupported: %d / %d\n", unsupported, len(cases))
}
