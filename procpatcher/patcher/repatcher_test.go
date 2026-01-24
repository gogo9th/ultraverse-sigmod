package patcher

import (
	"strings"
	"testing"
)

func TestRepatchLegacyCallinfo(t *testing.T) {
	sql := strings.Join([]string{
		"DELIMITER //",
		"CREATE PROCEDURE LegacyProc()",
		"BEGIN",
		"  -- INSERT INTO __ULTRAVERSE_PROCEDURE_HINT (procname) VALUES ('LegacyProc');",
		"  DECLARE __ultraverse_callinfo VARCHAR(512) DEFAULT JSON_ARRAY(UUID_SHORT(), 'LegacyProc');",
		"  INSERT INTO __ULTRAVERSE_PROCEDURE_HINT (callinfo) VALUES (__ultraverse_callinfo);",
		"  SELECT 1;",
		"END//",
		"DELIMITER ;",
		"",
	}, "\n")

	res, err := Repatch(sql)
	if err != nil {
		t.Fatalf("repatch failed: %v", err)
	}
	if strings.Contains(strings.ToLower(res.PatchedSQL), "__ultraverse_callinfo") {
		t.Fatalf("legacy callinfo should be removed during repatch")
	}
	if !strings.Contains(res.PatchedSQL, "INSERT INTO __ULTRAVERSE_PROCEDURE_HINT (callid, procname, args, vars)") {
		t.Fatalf("expected modern hint insert after repatch")
	}
	if !strings.Contains(res.PatchedSQL, "-- INSERT INTO __ULTRAVERSE_PROCEDURE_HINT") {
		t.Fatalf("commented insert line should remain")
	}
}

func TestRepatchKeepsSingleInsert(t *testing.T) {
	sql := strings.Join([]string{
		"DELIMITER //",
		"CREATE PROCEDURE AlreadyPatched()",
		"BEGIN",
		"  INSERT INTO __ULTRAVERSE_PROCEDURE_HINT (callid, procname, args, vars) VALUES (UUID_SHORT(), 'AlreadyPatched', JSON_OBJECT(), JSON_OBJECT());",
		"  SELECT 1;",
		"END//",
		"DELIMITER ;",
		"",
	}, "\n")

	res, err := Repatch(sql)
	if err != nil {
		t.Fatalf("repatch failed: %v", err)
	}
	if strings.Count(res.PatchedSQL, "INSERT INTO __ULTRAVERSE_PROCEDURE_HINT") != 1 {
		t.Fatalf("expected a single hint insert after repatch")
	}
}
