package parser

import (
	"bytes"
	"testing"

	pb "parserlib/pb"
)

func TestParseSimpleSelect(t *testing.T) {
	p := New()
	result := p.Parse("SELECT 1")

	if result.Result != pb.ParseResult_SUCCESS {
		t.Fatalf("expected SUCCESS, got %v: %s", result.Result, result.Error)
	}

	if len(result.Statements) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(result.Statements))
	}
}

func TestParseSelectWithWhere(t *testing.T) {
	p := New()
	result := p.Parse("SELECT * FROM users WHERE id = 1")

	if result.Result != pb.ParseResult_SUCCESS {
		t.Fatalf("expected SUCCESS, got %v: %s", result.Result, result.Error)
	}

	stmt := result.Statements[0]
	if stmt.Type != pb.Query_DML {
		t.Fatalf("expected DML query, got %v", stmt.Type)
	}

	dml := stmt.Dml
	if dml.Type != pb.DMLQuery_SELECT {
		t.Fatalf("expected SELECT, got %v", dml.Type)
	}

	if dml.Table.Real.Identifier != "users" {
		t.Fatalf("expected table 'users', got %s", dml.Table.Real.Identifier)
	}

	if dml.Where == nil {
		t.Fatal("expected WHERE clause")
	}
}

func TestParseInsert(t *testing.T) {
	p := New()
	result := p.Parse("INSERT INTO users (id, name) VALUES (1, 'test')")

	if result.Result != pb.ParseResult_SUCCESS {
		t.Fatalf("expected SUCCESS, got %v: %s", result.Result, result.Error)
	}

	dml := result.Statements[0].Dml
	if dml.Type != pb.DMLQuery_INSERT {
		t.Fatalf("expected INSERT, got %v", dml.Type)
	}

	if len(dml.UpdateOrWrite) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(dml.UpdateOrWrite))
	}
}

func TestParseUpdate(t *testing.T) {
	p := New()
	result := p.Parse("UPDATE users SET name = 'new' WHERE id = 1")

	if result.Result != pb.ParseResult_SUCCESS {
		t.Fatalf("expected SUCCESS, got %v: %s", result.Result, result.Error)
	}

	dml := result.Statements[0].Dml
	if dml.Type != pb.DMLQuery_UPDATE {
		t.Fatalf("expected UPDATE, got %v", dml.Type)
	}
}

func TestParseDelete(t *testing.T) {
	p := New()
	result := p.Parse("DELETE FROM users WHERE id = 1")

	if result.Result != pb.ParseResult_SUCCESS {
		t.Fatalf("expected SUCCESS, got %v: %s", result.Result, result.Error)
	}

	dml := result.Statements[0].Dml
	if dml.Type != pb.DMLQuery_DELETE {
		t.Fatalf("expected DELETE, got %v", dml.Type)
	}
}

func TestParseSelectIntoVariables(t *testing.T) {
	p := New()
	result := p.Parse("SELECT score INTO game_score FROM game_records WHERE user_id = 1")

	if result.Result != pb.ParseResult_SUCCESS {
		t.Fatalf("expected SUCCESS, got %v: %s", result.Result, result.Error)
	}

	dml := result.Statements[0].Dml
	if dml.Type != pb.DMLQuery_SELECT {
		t.Fatalf("expected SELECT, got %v", dml.Type)
	}

	if len(dml.IntoVariables) != 1 {
		t.Fatalf("expected 1 INTO variable, got %d", len(dml.IntoVariables))
	}

	if dml.IntoVariables[0] != "game_score" {
		t.Fatalf("expected INTO variable 'game_score', got %s", dml.IntoVariables[0])
	}
}

func TestParseSelectIntoMultipleVariables(t *testing.T) {
	p := New()
	result := p.Parse("SELECT col1, col2 INTO var1, var2 FROM t WHERE id = 1")

	if result.Result != pb.ParseResult_SUCCESS {
		t.Fatalf("expected SUCCESS, got %v: %s", result.Result, result.Error)
	}

	dml := result.Statements[0].Dml
	if len(dml.IntoVariables) != 2 {
		t.Fatalf("expected 2 INTO variables, got %d", len(dml.IntoVariables))
	}

	if dml.IntoVariables[0] != "var1" || dml.IntoVariables[1] != "var2" {
		t.Fatalf("expected INTO variables [var1, var2], got %v", dml.IntoVariables)
	}
}

func TestHashSameQuery(t *testing.T) {
	p := New()

	hash1, err1 := p.Hash("SELECT * FROM users WHERE id = 1")
	hash2, err2 := p.Hash("SELECT  *  FROM  users  WHERE  id  =  1")

	if err1 != nil || err2 != nil {
		t.Fatalf("hash error: %v, %v", err1, err2)
	}

	if !bytes.Equal(hash1, hash2) {
		t.Fatal("expected same hash for equivalent queries")
	}
}

func TestHashDifferentQuery(t *testing.T) {
	p := New()

	hash1, err1 := p.Hash("SELECT * FROM users WHERE id = 1")
	hash2, err2 := p.Hash("SELECT * FROM users WHERE id = 2")

	if err1 != nil || err2 != nil {
		t.Fatalf("hash error: %v, %v", err1, err2)
	}

	if bytes.Equal(hash1, hash2) {
		t.Fatal("expected different hash for different queries")
	}
}

func TestJsonify(t *testing.T) {
	p := New()

	jsonBytes, err := p.Jsonify("SELECT 1")
	if err != nil {
		t.Fatalf("jsonify error: %v", err)
	}

	if len(jsonBytes) == 0 {
		t.Fatal("expected non-empty JSON output")
	}
}

func TestParseProcedure(t *testing.T) {
	p := New()
	sql := `CREATE PROCEDURE test_proc()
BEGIN
  SELECT score INTO game_score FROM game_records WHERE user_id = 1;
END`

	result := p.Parse(sql)
	if result.Result != pb.ParseResult_SUCCESS {
		t.Fatalf("expected SUCCESS, got %v: %s", result.Result, result.Error)
	}

	stmt := result.Statements[0]
	if stmt.Type != pb.Query_PROCEDURE {
		t.Fatalf("expected PROCEDURE, got %v", stmt.Type)
	}

	proc := stmt.Procedure
	if proc.Name != "test_proc" {
		t.Fatalf("expected procedure name 'test_proc', got %s", proc.Name)
	}
}

func TestParseFunctionCall(t *testing.T) {
	p := New()
	result := p.Parse("SELECT NOW()")

	if result.Result != pb.ParseResult_SUCCESS {
		t.Fatalf("expected SUCCESS, got %v: %s", result.Result, result.Error)
	}

	dml := result.Statements[0].Dml
	if len(dml.Select) != 1 {
		t.Fatalf("expected 1 select field, got %d", len(dml.Select))
	}

	field := dml.Select[0].Real
	if field.ValueType != pb.DMLQueryExpr_FUNCTION {
		t.Fatalf("expected FUNCTION, got %v", field.ValueType)
	}
}

func TestParseDecimalValue(t *testing.T) {
	p := New()
	result := p.Parse("UPDATE warehouse SET W_YTD = 3980.34 WHERE W_ID = 10")

	if result.Result != pb.ParseResult_SUCCESS {
		t.Fatalf("expected SUCCESS, got %v: %s", result.Result, result.Error)
	}

	dml := result.Statements[0].Dml
	assignment := dml.UpdateOrWrite[0]
	if assignment.Right.ValueType != pb.DMLQueryExpr_DECIMAL {
		t.Fatalf("expected DECIMAL, got %v", assignment.Right.ValueType)
	}

	if assignment.Right.Decimal != "3980.34" {
		t.Fatalf("expected decimal '3980.34', got %s", assignment.Right.Decimal)
	}
}

func TestParseInvalidSQL(t *testing.T) {
	p := New()
	result := p.Parse("SELCT * FORM users") // intentional typos

	if result.Result != pb.ParseResult_ERROR {
		t.Fatal("expected ERROR for invalid SQL")
	}

	if result.Error == "" {
		t.Fatal("expected error message")
	}
}
