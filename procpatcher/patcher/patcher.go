package patcher

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"

	"procpatcher/delimiter"
)

// PatchResult contains the result of patching
type PatchResult struct {
	PatchedSQL string
	Warnings   []string
}

// Patch patches all stored procedures in the SQL with hint inserts
func Patch(sql string) (*PatchResult, error) {
	result := &PatchResult{
		Warnings: []string{},
	}

	statements, err := delimiter.SplitStatements(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to split statements: %w", err)
	}

	p := parser.New()
	var insertions []textInsertion
	seq := 0

	for _, stmt := range statements {
		if !stmt.HasCode {
			continue
		}
		parsed, err := p.ParseOneStmt(stmt.Text, "", "")
		if err != nil {
			result.Warnings = append(result.Warnings,
				fmt.Sprintf("Warning: failed to parse statement at offset %d, leaving unchanged: %v", stmt.Start, err))
			continue
		}

		procInfo, ok := parsed.(*ast.ProcedureInfo)
		if !ok {
			continue
		}

		// Get procedure name
		procName := ""
		if procInfo.ProcedureName != nil {
			procName = procInfo.ProcedureName.Name.O
		}

		// Check if already patched (idempotency)
		if hasHintTableReference(stmt.Text) {
			result.Warnings = append(result.Warnings,
				fmt.Sprintf("Warning: Procedure '%s' already patched, skipping", procName))
			continue
		}

		// Extract parameters
		params := ExtractParameters(procInfo)

		// Get the procedure body
		if procInfo.ProcedureBody == nil {
			result.Warnings = append(result.Warnings,
				fmt.Sprintf("Warning: Procedure '%s' has no body, skipping", procName))
			continue
		}

		// Get the block from the body
		var block *ast.ProcedureBlock
		var label string

		switch body := procInfo.ProcedureBody.(type) {
		case *ast.ProcedureLabelBlock:
			block = body.Block
			label = body.LabelName
		case *ast.ProcedureBlock:
			block = body
		default:
			result.Warnings = append(result.Warnings,
				fmt.Sprintf("Warning: Procedure '%s' has unsupported body type: %T, skipping", procName, body))
			continue
		}

		if block == nil {
			result.Warnings = append(result.Warnings,
				fmt.Sprintf("Warning: Procedure '%s' has nil block, skipping", procName))
			continue
		}

		inserter := NewProcedureInserter(procName, params, label)
		inserter.ProcessBlock(block, label, true)
		newline := detectNewline(stmt.Text)

		for _, ip := range inserter.GetInsertionPoints() {
			lineStart := findLineStart(stmt.Text, ip.Stmt.OriginTextPosition())
			indent := lineIndent(stmt.Text, lineStart)
			localVars := visibleLocals(ip.Scope, params)
			insertSQL := BuildHintInsertSQL(procName, params, localVars)
			insertText := indent + insertSQL + ";" + newline
			insertions = append(insertions, textInsertion{
				Offset: stmt.Start + lineStart,
				Text:   insertText,
				Seq:    seq,
			})
			seq++
		}

		endLineStart, endIndent, ok := findProcedureEndInsertion(stmt.Text)
		if !ok {
			result.Warnings = append(result.Warnings,
				fmt.Sprintf("Warning: Procedure '%s' end not found, skipping end insert", procName))
			continue
		}
		insertSQL := BuildHintInsertSQL(procName, params, visibleLocals(inserter.GetRootBlockScope(), params))
		insertText := endIndent + insertSQL + ";" + newline
		insertions = append(insertions, textInsertion{
			Offset: stmt.Start + endLineStart,
			Text:   insertText,
			Seq:    seq,
		})
		seq++
	}

	result.PatchedSQL = applyInsertions(sql, insertions)

	return result, nil
}
