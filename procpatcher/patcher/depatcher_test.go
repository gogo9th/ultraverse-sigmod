package patcher

import (
	"strings"
	"testing"
)

func TestDepatchRemovesHintInsert(t *testing.T) {
	sql := strings.Join([]string{
		"DELIMITER //",
		"CREATE PROCEDURE P()",
		"BEGIN",
		"  SELECT 1;",
		"  INSERT INTO __ULTRAVERSE_PROCEDURE_HINT (callid, procname, args, vars) VALUES (UUID_SHORT(), 'P', JSON_OBJECT(), JSON_OBJECT());",
		"  SELECT 2;",
		"END//",
		"DELIMITER ;",
		"",
	}, "\n")

	res, err := Depatch(sql)
	if err != nil {
		t.Fatalf("depatch failed: %v", err)
	}
	if strings.Contains(res.PatchedSQL, "INSERT INTO __ULTRAVERSE_PROCEDURE_HINT") {
		t.Fatalf("hint insert was not removed")
	}
	if !strings.Contains(res.PatchedSQL, "DELIMITER //") {
		t.Fatalf("delimiter directive was removed")
	}
	if !strings.Contains(res.PatchedSQL, "SELECT 2;") {
		t.Fatalf("expected statement missing after depatch")
	}
}

func TestDepatchRemovesLegacyCallinfo(t *testing.T) {
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

	res, err := Depatch(sql)
	if err != nil {
		t.Fatalf("depatch failed: %v", err)
	}
	if strings.Contains(strings.ToLower(res.PatchedSQL), "declare __ultraverse_callinfo") {
		t.Fatalf("legacy callinfo DECLARE was not removed")
	}
	if strings.Contains(strings.ToLower(res.PatchedSQL), "insert into __ultraverse_procedure_hint (callinfo)") {
		t.Fatalf("legacy callinfo INSERT was not removed")
	}
	if !strings.Contains(res.PatchedSQL, "-- INSERT INTO __ULTRAVERSE_PROCEDURE_HINT") {
		t.Fatalf("commented insert line should remain")
	}
}

func TestDepatchRemovesProcnameInsertInFunction(t *testing.T) {
	sql := strings.Join([]string{
		"DELIMITER //",
		"CREATE FUNCTION RandFunc(minval INT, maxval INT) RETURNS FLOAT",
		"BEGIN",
		"  INSERT INTO __ULTRAVERSE_PROCEDURE_HINT (procname) VALUES ('RandFunc');",
		"  RETURN 1;",
		"END//",
		"DELIMITER ;",
		"",
	}, "\n")

	res, err := Depatch(sql)
	if err != nil {
		t.Fatalf("depatch failed: %v", err)
	}
	if strings.Contains(res.PatchedSQL, "INSERT INTO __ULTRAVERSE_PROCEDURE_HINT") {
		t.Fatalf("procname insert was not removed")
	}
	if !strings.Contains(res.PatchedSQL, "RETURN 1;") {
		t.Fatalf("function body was altered unexpectedly")
	}
}

func TestPatchThenDepatchRoundTrip(t *testing.T) {
	orig := strings.Join([]string{
		"DELIMITER //",
		"CREATE PROCEDURE RoundTrip()",
		"RoundTrip_Label:BEGIN",
		"  DECLARE v INT DEFAULT 0;",
		"  IF v = 0 THEN",
		"    LEAVE RoundTrip_Label;",
		"  END IF;",
		"END//",
		"DELIMITER ;",
		"",
	}, "\n")

	patched, err := Patch(orig)
	if err != nil {
		t.Fatalf("patch failed: %v", err)
	}
	depatched, err := Depatch(patched.PatchedSQL)
	if err != nil {
		t.Fatalf("depatch failed: %v", err)
	}
	if strings.Contains(depatched.PatchedSQL, "INSERT INTO __ULTRAVERSE_PROCEDURE_HINT") {
		t.Fatalf("round-trip left hint insert behind")
	}
	if !strings.Contains(depatched.PatchedSQL, "LEAVE RoundTrip_Label;") {
		t.Fatalf("round-trip removed original statement")
	}
}
