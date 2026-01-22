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
			Name: "START TRANSACTION",
			SQL: `CREATE PROCEDURE test_proc()
BEGIN
    START TRANSACTION;
    SELECT 1;
    COMMIT;
END;`,
		},
		{
			Name: "DECLARE CONDITION FOR SQLSTATE",
			SQL: `CREATE PROCEDURE test_proc()
BEGIN
    DECLARE insufficient_funds CONDITION FOR SQLSTATE '45002';
    SELECT 1;
END;`,
		},
		{
			Name: "SIGNAL with SQLSTATE",
			SQL: `CREATE PROCEDURE test_proc()
BEGIN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Error occurred';
END;`,
		},
		{
			Name: "RESIGNAL",
			SQL: `CREATE PROCEDURE test_proc()
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        RESIGNAL;
    END;
    SELECT 1;
END;`,
		},
		{
			Name: "GET DIAGNOSTICS",
			SQL: `CREATE PROCEDURE test_proc()
BEGIN
    DECLARE v_errno INT;
    DECLARE v_msg TEXT;
    GET DIAGNOSTICS CONDITION 1
        v_errno = MYSQL_ERRNO,
        v_msg = MESSAGE_TEXT;
END;`,
		},
		{
			Name: "DECLARE CURSOR",
			SQL: `CREATE PROCEDURE test_proc()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE cur CURSOR FOR SELECT id FROM users;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    OPEN cur;
    CLOSE cur;
END;`,
		},
		{
			Name: "FETCH CURSOR",
			SQL: `CREATE PROCEDURE test_proc()
BEGIN
    DECLARE v_id INT;
    DECLARE cur CURSOR FOR SELECT id FROM users;
    OPEN cur;
    FETCH cur INTO v_id;
    CLOSE cur;
END;`,
		},
		{
			Name: "REPEAT UNTIL",
			SQL: `CREATE PROCEDURE test_proc()
BEGIN
    DECLARE i INT DEFAULT 0;
    REPEAT
        SET i = i + 1;
    UNTIL i >= 10
    END REPEAT;
END;`,
		},
		{
			Name: "LOOP with ITERATE",
			SQL: `CREATE PROCEDURE test_proc()
BEGIN
    DECLARE i INT DEFAULT 0;
    my_loop: LOOP
        SET i = i + 1;
        IF i < 5 THEN
            ITERATE my_loop;
        END IF;
        LEAVE my_loop;
    END LOOP my_loop;
END;`,
		},
		{
			Name: "CASE statement (procedure style)",
			SQL: `CREATE PROCEDURE test_proc(IN val INT)
BEGIN
    CASE val
        WHEN 1 THEN SELECT 'one';
        WHEN 2 THEN SELECT 'two';
        ELSE SELECT 'other';
    END CASE;
END;`,
		},
		{
			Name: "Multiple HANDLERs",
			SQL: `CREATE PROCEDURE test_proc()
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION SELECT 'SQL Exception';
    DECLARE EXIT HANDLER FOR SQLWARNING SELECT 'SQL Warning';
    DECLARE CONTINUE HANDLER FOR NOT FOUND SELECT 'Not Found';
    SELECT 1;
END;`,
		},
		{
			Name: "SAVEPOINT and ROLLBACK TO",
			SQL: `CREATE PROCEDURE test_proc()
BEGIN
    START TRANSACTION;
    SAVEPOINT sp1;
    INSERT INTO t1 VALUES (1);
    ROLLBACK TO sp1;
    COMMIT;
END;`,
		},
		{
			Name: "PREPARE and EXECUTE (dynamic SQL)",
			SQL: `CREATE PROCEDURE test_proc(IN tbl_name VARCHAR(64))
BEGIN
    SET @sql = CONCAT('SELECT * FROM ', tbl_name);
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END;`,
		},
		{
			Name: "OUT parameter with SELECT INTO",
			SQL: `CREATE PROCEDURE test_proc(IN p_id INT, OUT p_name VARCHAR(100))
BEGIN
    SELECT name INTO p_name FROM users WHERE id = p_id;
END;`,
		},
		{
			Name: "Nested BEGIN END blocks",
			SQL: `CREATE PROCEDURE test_proc()
outer_block: BEGIN
    DECLARE v1 INT;
    inner_block: BEGIN
        DECLARE v2 INT;
        SET v2 = 1;
    END inner_block;
    SET v1 = 2;
END outer_block;`,
		},
		{
			Name: "DECLARE with DEFAULT expression",
			SQL: `CREATE PROCEDURE test_proc()
BEGIN
    DECLARE v_now DATETIME DEFAULT NOW();
    DECLARE v_uuid VARCHAR(36) DEFAULT UUID();
    SELECT v_now, v_uuid;
END;`,
		},
		{
			Name: "FOR loop (MariaDB style)",
			SQL: `CREATE PROCEDURE test_proc()
BEGIN
    FOR i IN 1..10 DO
        SELECT i;
    END FOR;
END;`,
		},
		{
			Name: "RETURN in function (not procedure)",
			SQL: `CREATE FUNCTION test_func(p INT) RETURNS INT
BEGIN
    RETURN p * 2;
END;`,
		},
	}

	p := parser.New()

	fmt.Println("=== tidb/parser Procedure Syntax Support Test ===")
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
