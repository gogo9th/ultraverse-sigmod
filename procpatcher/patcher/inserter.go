package patcher

import (
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// InsertionPoint represents a point where we need to insert the hint
type InsertionPoint struct {
	Type  InsertionType
	Scope *Scope
	Stmt  ast.StmtNode
}

type InsertionType int

const (
	InsertBeforeEarlyExit InsertionType = iota
)

// ProcedureInserter finds insertion points and modifies the AST
type ProcedureInserter struct {
	procName     string
	params       []Variable
	procLabel    string
	scopeTracker *ScopeTracker
	insertPoints []InsertionPoint
	rootScope    *Scope
}

// NewProcedureInserter creates a new inserter for a procedure
func NewProcedureInserter(procName string, params []Variable, procLabel string) *ProcedureInserter {
	return &ProcedureInserter{
		procName:     procName,
		params:       params,
		procLabel:    procLabel,
		scopeTracker: NewScopeTracker(params),
		insertPoints: []InsertionPoint{},
	}
}

// ProcessBlock processes a BEGIN...END block and finds insertion points
func (pi *ProcedureInserter) ProcessBlock(block *ast.ProcedureBlock, label string, isRoot bool) {
	if block == nil {
		return
	}

	// Extract declared variables from the block
	declaredVars := ExtractDeclaredVariables(block)

	// Enter new scope
	pi.scopeTracker.EnterBlock(label, declaredVars)
	if isRoot {
		pi.rootScope = pi.scopeTracker.GetCurrentScope()
	}

	// Process statements to find insertion points
	pi.processBlockStatements(block.ProcedureProcStmts)

	// Leave scope
	pi.scopeTracker.LeaveBlock()
}

// ProcessLabeledBlock processes a labeled BEGIN...END block
func (pi *ProcedureInserter) ProcessLabeledBlock(labelBlock *ast.ProcedureLabelBlock) {
	if labelBlock == nil || labelBlock.Block == nil {
		return
	}

	label := labelBlock.LabelName
	pi.ProcessBlock(labelBlock.Block, label, false)
}

// processBlockStatements processes statements inside a BEGIN...END block
func (pi *ProcedureInserter) processBlockStatements(stmts []ast.StmtNode) {
	for _, stmt := range stmts {
		switch s := stmt.(type) {
		case *ast.ProcedureLabelBlock:
			// Labeled BEGIN...END block
			pi.ProcessLabeledBlock(s)

		case *ast.ProcedureJump:
			// LEAVE or ITERATE statement
			if s.IsLeave && pi.isProcExitLabel(s.Name) {
				// LEAVE statement - need to insert before it
				pi.insertPoints = append(pi.insertPoints, InsertionPoint{
					Type:  InsertBeforeEarlyExit,
					Scope: pi.scopeTracker.GetCurrentScope(),
					Stmt:  s,
				})
			}

		case *ast.ProcedureSignalStmt:
			pi.insertPoints = append(pi.insertPoints, InsertionPoint{
				Type:  InsertBeforeEarlyExit,
				Scope: pi.scopeTracker.GetCurrentScope(),
				Stmt:  s,
			})

		case *ast.ProcedureReturnStmt:
			pi.insertPoints = append(pi.insertPoints, InsertionPoint{
				Type:  InsertBeforeEarlyExit,
				Scope: pi.scopeTracker.GetCurrentScope(),
				Stmt:  s,
			})

		case *ast.ProcedureIfInfo:
			// Process IF branches
			pi.processIfInfo(s)

		case *ast.ProcedureLabelLoop:
			// Process labeled loop
			pi.processLabelLoop(s)

		case *ast.ProcedureWhileStmt:
			// Process WHILE body (no new scope, just find LEAVE statements)
			pi.processBlockStatements(s.Body)

		case *ast.ProcedureRepeatStmt:
			// Process REPEAT body
			pi.processBlockStatements(s.Body)
		}
	}
}

// processIfStatements processes statements inside an IF THEN branch
func (pi *ProcedureInserter) processIfStatements(ifBlock *ast.ProcedureIfBlock, stmts []ast.StmtNode) {
	for _, stmt := range stmts {
		switch s := stmt.(type) {
		case *ast.ProcedureLabelBlock:
			pi.ProcessLabeledBlock(s)

		case *ast.ProcedureJump:
			if s.IsLeave && pi.isProcExitLabel(s.Name) {
				pi.insertPoints = append(pi.insertPoints, InsertionPoint{
					Type:  InsertBeforeEarlyExit,
					Scope: pi.scopeTracker.GetCurrentScope(),
					Stmt:  s,
				})
			}

		case *ast.ProcedureSignalStmt:
			pi.insertPoints = append(pi.insertPoints, InsertionPoint{
				Type:  InsertBeforeEarlyExit,
				Scope: pi.scopeTracker.GetCurrentScope(),
				Stmt:  s,
			})

		case *ast.ProcedureReturnStmt:
			pi.insertPoints = append(pi.insertPoints, InsertionPoint{
				Type:  InsertBeforeEarlyExit,
				Scope: pi.scopeTracker.GetCurrentScope(),
				Stmt:  s,
			})

		case *ast.ProcedureIfInfo:
			pi.processIfInfo(s)

		case *ast.ProcedureLabelLoop:
			pi.processLabelLoop(s)

		case *ast.ProcedureWhileStmt:
			pi.processIfStatements(ifBlock, s.Body)

		case *ast.ProcedureRepeatStmt:
			pi.processIfStatements(ifBlock, s.Body)
		}
	}
}

// processElseStatements processes statements inside an ELSE branch
func (pi *ProcedureInserter) processElseStatements(elseBlock *ast.ProcedureElseBlock, stmts []ast.StmtNode) {
	for _, stmt := range stmts {
		switch s := stmt.(type) {
		case *ast.ProcedureLabelBlock:
			pi.ProcessLabeledBlock(s)

		case *ast.ProcedureJump:
			if s.IsLeave && pi.isProcExitLabel(s.Name) {
				pi.insertPoints = append(pi.insertPoints, InsertionPoint{
					Type:  InsertBeforeEarlyExit,
					Scope: pi.scopeTracker.GetCurrentScope(),
					Stmt:  s,
				})
			}

		case *ast.ProcedureSignalStmt:
			pi.insertPoints = append(pi.insertPoints, InsertionPoint{
				Type:  InsertBeforeEarlyExit,
				Scope: pi.scopeTracker.GetCurrentScope(),
				Stmt:  s,
			})

		case *ast.ProcedureReturnStmt:
			pi.insertPoints = append(pi.insertPoints, InsertionPoint{
				Type:  InsertBeforeEarlyExit,
				Scope: pi.scopeTracker.GetCurrentScope(),
				Stmt:  s,
			})

		case *ast.ProcedureIfInfo:
			pi.processIfInfo(s)

		case *ast.ProcedureLabelLoop:
			pi.processLabelLoop(s)

		case *ast.ProcedureWhileStmt:
			pi.processElseStatements(elseBlock, s.Body)

		case *ast.ProcedureRepeatStmt:
			pi.processElseStatements(elseBlock, s.Body)
		}
	}
}

// processIfInfo processes IF statement branches
func (pi *ProcedureInserter) processIfInfo(info *ast.ProcedureIfInfo) {
	if info == nil || info.IfBody == nil {
		return
	}

	pi.processIfBlock(info.IfBody)
}

// processIfBlock processes an IF block (THEN and ELSE branches)
func (pi *ProcedureInserter) processIfBlock(block *ast.ProcedureIfBlock) {
	if block == nil {
		return
	}

	// Process THEN branch statements - look for LEAVE and nested structures
	pi.processIfStatements(block, block.ProcedureIfStmts)

	// Process ELSE branch (could be another IF block or ELSE block)
	if block.ProcedureElseStmt != nil {
		switch elseStmt := block.ProcedureElseStmt.(type) {
		case *ast.ProcedureElseIfBlock:
			// ELSEIF - recursively process
			pi.processIfBlock(elseStmt.ProcedureIfStmt)
		case *ast.ProcedureElseBlock:
			// ELSE branch
			pi.processElseStatements(elseStmt, elseStmt.ProcedureIfStmts)
		}
	}
}

// processLabelLoop processes a labeled loop
func (pi *ProcedureInserter) processLabelLoop(loop *ast.ProcedureLabelLoop) {
	if loop == nil || loop.Block == nil {
		return
	}

	// For labeled loops, we need to process the body to find LEAVE statements
	// Note: LEAVE inside a loop exits the loop, not the procedure
	// But we still need to traverse to find nested structures
	switch block := loop.Block.(type) {
	case *ast.ProcedureWhileStmt:
		pi.processLoopBody(block.Body)
	case *ast.ProcedureRepeatStmt:
		pi.processLoopBody(block.Body)
	}
}

// processLoopBody processes statements inside a loop body
// LEAVE statements inside loops that target the loop label should NOT get hint inserts
// (they just exit the loop, not the procedure)
// But LEAVE statements targeting outer labels (like procedure label) should get hint inserts
func (pi *ProcedureInserter) processLoopBody(stmts []ast.StmtNode) {
	for _, stmt := range stmts {
		switch s := stmt.(type) {
		case *ast.ProcedureLabelBlock:
			pi.ProcessLabeledBlock(s)

		case *ast.ProcedureJump:
			if s.IsLeave && pi.isProcExitLabel(s.Name) {
				pi.insertPoints = append(pi.insertPoints, InsertionPoint{
					Type:  InsertBeforeEarlyExit,
					Scope: pi.scopeTracker.GetCurrentScope(),
					Stmt:  s,
				})
			}

		case *ast.ProcedureSignalStmt:
			pi.insertPoints = append(pi.insertPoints, InsertionPoint{
				Type:  InsertBeforeEarlyExit,
				Scope: pi.scopeTracker.GetCurrentScope(),
				Stmt:  s,
			})

		case *ast.ProcedureReturnStmt:
			pi.insertPoints = append(pi.insertPoints, InsertionPoint{
				Type:  InsertBeforeEarlyExit,
				Scope: pi.scopeTracker.GetCurrentScope(),
				Stmt:  s,
			})

		case *ast.ProcedureIfInfo:
			pi.processIfInfo(s)

		case *ast.ProcedureLabelLoop:
			pi.processLabelLoop(s)

		case *ast.ProcedureWhileStmt:
			pi.processLoopBody(s.Body)

		case *ast.ProcedureRepeatStmt:
			pi.processLoopBody(s.Body)

			// Note: only LEAVE targeting the procedure label is inserted.
		}
	}
}

// GetInsertionPoints returns all found insertion points
func (pi *ProcedureInserter) GetInsertionPoints() []InsertionPoint {
	return pi.insertPoints
}

// GetProcName returns the procedure name
func (pi *ProcedureInserter) GetProcName() string {
	return pi.procName
}

// GetParams returns the procedure parameters
func (pi *ProcedureInserter) GetParams() []Variable {
	return pi.params
}

// GetRootBlockScope returns the root block scope (outermost BEGIN...END).
func (pi *ProcedureInserter) GetRootBlockScope() *Scope {
	return pi.rootScope
}

func (pi *ProcedureInserter) isProcExitLabel(label string) bool {
	if pi.procLabel == "" {
		return false
	}
	return strings.EqualFold(pi.procLabel, label)
}
