package patcher

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"

	"procpatcher/delimiter"
)

// PatchResult contains the result of patching
type PatchResult struct {
	PatchedSQL string
	Warnings   []string
}

// hintTablePattern matches INSERT INTO __ULTRAVERSE_PROCEDURE_HINT
var hintTablePattern = regexp.MustCompile(`(?i)__ULTRAVERSE_PROCEDURE_HINT`)

// Patch patches all stored procedures in the SQL with hint inserts
func Patch(sql string) (*PatchResult, error) {
	result := &PatchResult{
		Warnings: []string{},
	}

	// Step 1: Remove DELIMITER statements (tidb/parser doesn't support them)
	cleanSQL, delimiters := delimiter.RemoveDelimiters(sql)

	// Step 2: Parse the SQL
	p := parser.New()
	stmts, _, err := p.Parse(cleanSQL, "", "")
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}

	// Step 3: Find and patch ProcedureInfo statements
	for _, stmt := range stmts {
		procInfo, ok := stmt.(*ast.ProcedureInfo)
		if !ok {
			continue
		}

		// Get procedure name
		procName := ""
		if procInfo.ProcedureName != nil {
			procName = procInfo.ProcedureName.Name.O
		}

		// Check if already patched (idempotency)
		procSQL := restoreNode(procInfo)
		if hintTablePattern.MatchString(procSQL) {
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

		// Find insertion points
		inserter := NewProcedureInserter(procName, params)
		inserter.ProcessBlock(block, label)

		// Modify the AST by inserting hint statements
		insertPoints := inserter.GetInsertionPoints()
		modifyAST(procName, params, insertPoints)
	}

	// Step 4: Restore the modified SQL
	var outputBuilder strings.Builder
	for i, stmt := range stmts {
		if i > 0 {
			outputBuilder.WriteString("\n\n")
		}

		restored := restoreNode(stmt)

		// Format procedures and functions for readability
		switch stmt.(type) {
		case *ast.ProcedureInfo, *ast.FunctionInfo:
			restored = FormatProcedureSQL(restored)
		}

		outputBuilder.WriteString(restored)
		outputBuilder.WriteString(";")
	}

	patchedCleanSQL := outputBuilder.String()

	// Step 5: Restore DELIMITER statements
	result.PatchedSQL = delimiter.RestoreDelimiters(patchedCleanSQL, delimiters)

	return result, nil
}

// modifyAST inserts hint statements at the insertion points
func modifyAST(procName string, params []Variable, insertPoints []InsertionPoint) {
	// Group insertion points by their container type
	blockInserts := make(map[*ast.ProcedureBlock][]InsertionPoint)
	ifBlockInserts := make(map[*ast.ProcedureIfBlock][]InsertionPoint)
	elseBlockInserts := make(map[*ast.ProcedureElseBlock][]InsertionPoint)

	for _, ip := range insertPoints {
		if ip.BlockStmt != nil {
			blockInserts[ip.BlockStmt] = append(blockInserts[ip.BlockStmt], ip)
		} else if ip.IfBlock != nil {
			ifBlockInserts[ip.IfBlock] = append(ifBlockInserts[ip.IfBlock], ip)
		} else if ip.ElseBlock != nil {
			elseBlockInserts[ip.ElseBlock] = append(elseBlockInserts[ip.ElseBlock], ip)
		}
	}

	// Helper to build INSERT statement for an insertion point
	buildInsertStmt := func(ip InsertionPoint) ast.StmtNode {
		// Get visible variables at this scope
		vars := ip.Scope.GetAllVisibleVariables()

		// Separate params from local vars
		paramSet := make(map[string]bool)
		for _, p := range params {
			paramSet[p.Name] = true
		}

		var localVars []Variable
		for _, v := range vars {
			if !paramSet[v.Name] {
				localVars = append(localVars, v)
			}
		}

		// Build the INSERT statement
		insertSQL := BuildHintInsertSQL(procName, params, localVars)
		return createInsertStmt(insertSQL)
	}

	// Process BEGIN...END blocks
	for block, points := range blockInserts {
		// Sort by index descending so we can insert without shifting affecting later inserts
		sort.Slice(points, func(i, j int) bool {
			return points[i].Index > points[j].Index
		})

		for _, ip := range points {
			insertStmt := buildInsertStmt(ip)
			if insertStmt == nil {
				continue
			}

			if ip.Type == InsertBeforeEnd {
				block.ProcedureProcStmts = append(block.ProcedureProcStmts, insertStmt)
			} else {
				newStmts := make([]ast.StmtNode, 0, len(block.ProcedureProcStmts)+1)
				newStmts = append(newStmts, block.ProcedureProcStmts[:ip.Index]...)
				newStmts = append(newStmts, insertStmt)
				newStmts = append(newStmts, block.ProcedureProcStmts[ip.Index:]...)
				block.ProcedureProcStmts = newStmts
			}
		}
	}

	// Process IF THEN blocks
	for ifBlock, points := range ifBlockInserts {
		sort.Slice(points, func(i, j int) bool {
			return points[i].Index > points[j].Index
		})

		for _, ip := range points {
			insertStmt := buildInsertStmt(ip)
			if insertStmt == nil {
				continue
			}

			// Insert before LEAVE statement
			newStmts := make([]ast.StmtNode, 0, len(ifBlock.ProcedureIfStmts)+1)
			newStmts = append(newStmts, ifBlock.ProcedureIfStmts[:ip.Index]...)
			newStmts = append(newStmts, insertStmt)
			newStmts = append(newStmts, ifBlock.ProcedureIfStmts[ip.Index:]...)
			ifBlock.ProcedureIfStmts = newStmts
		}
	}

	// Process ELSE blocks
	for elseBlock, points := range elseBlockInserts {
		sort.Slice(points, func(i, j int) bool {
			return points[i].Index > points[j].Index
		})

		for _, ip := range points {
			insertStmt := buildInsertStmt(ip)
			if insertStmt == nil {
				continue
			}

			// Insert before LEAVE statement
			newStmts := make([]ast.StmtNode, 0, len(elseBlock.ProcedureIfStmts)+1)
			newStmts = append(newStmts, elseBlock.ProcedureIfStmts[:ip.Index]...)
			newStmts = append(newStmts, insertStmt)
			newStmts = append(newStmts, elseBlock.ProcedureIfStmts[ip.Index:]...)
			elseBlock.ProcedureIfStmts = newStmts
		}
	}
}

// createInsertStmt creates an INSERT statement node
func createInsertStmt(sql string) ast.StmtNode {
	p := parser.New()
	stmts, _, err := p.Parse(sql, "", "")
	if err != nil || len(stmts) == 0 {
		return nil
	}
	return stmts[0]
}

// restoreNode converts an AST node back to SQL
func restoreNode(node ast.Node) string {
	var sb strings.Builder
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	if err := node.Restore(ctx); err != nil {
		return ""
	}
	return sb.String()
}
