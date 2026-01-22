package patcher

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// InsertionPoint represents a point where we need to insert the hint
type InsertionPoint struct {
	Type  InsertionType
	Scope *Scope
	Index int    // Index in the statement list
	Label string // For LEAVE statements, the target label

	// One of these will be set depending on the parent container
	BlockStmt *ast.ProcedureBlock    // BEGIN...END block
	IfBlock   *ast.ProcedureIfBlock  // IF/ELSEIF THEN branch
	ElseBlock *ast.ProcedureElseBlock // ELSE branch
}

type InsertionType int

const (
	InsertBeforeEnd InsertionType = iota
	InsertBeforeLeave
)

// ProcedureInserter finds insertion points and modifies the AST
type ProcedureInserter struct {
	procName     string
	params       []Variable
	scopeTracker *ScopeTracker
	insertPoints []InsertionPoint
}

// NewProcedureInserter creates a new inserter for a procedure
func NewProcedureInserter(procName string, params []Variable) *ProcedureInserter {
	return &ProcedureInserter{
		procName:     procName,
		params:       params,
		scopeTracker: NewScopeTracker(params),
		insertPoints: []InsertionPoint{},
	}
}

// ProcessBlock processes a BEGIN...END block and finds insertion points
func (pi *ProcedureInserter) ProcessBlock(block *ast.ProcedureBlock, label string) {
	if block == nil {
		return
	}

	// Extract declared variables from the block
	declaredVars := ExtractDeclaredVariables(block)

	// Enter new scope
	pi.scopeTracker.EnterBlock(label, declaredVars)

	// Process statements to find insertion points
	pi.processBlockStatements(block, block.ProcedureProcStmts)

	// The END of this block is an insertion point
	pi.insertPoints = append(pi.insertPoints, InsertionPoint{
		Type:      InsertBeforeEnd,
		Scope:     pi.scopeTracker.GetCurrentScope(),
		BlockStmt: block,
		Index:     len(block.ProcedureProcStmts), // Insert at the end
		Label:     label,
	})

	// Leave scope
	pi.scopeTracker.LeaveBlock()
}

// ProcessLabeledBlock processes a labeled BEGIN...END block
func (pi *ProcedureInserter) ProcessLabeledBlock(labelBlock *ast.ProcedureLabelBlock) {
	if labelBlock == nil || labelBlock.Block == nil {
		return
	}

	label := labelBlock.LabelName
	pi.ProcessBlock(labelBlock.Block, label)
}

// processBlockStatements processes statements inside a BEGIN...END block
func (pi *ProcedureInserter) processBlockStatements(parentBlock *ast.ProcedureBlock, stmts []ast.StmtNode) {
	for i, stmt := range stmts {
		switch s := stmt.(type) {
		case *ast.ProcedureLabelBlock:
			// Labeled BEGIN...END block
			pi.ProcessLabeledBlock(s)

		case *ast.ProcedureJump:
			// LEAVE or ITERATE statement
			if s.IsLeave {
				// LEAVE statement - need to insert before it
				pi.insertPoints = append(pi.insertPoints, InsertionPoint{
					Type:      InsertBeforeLeave,
					Scope:     pi.scopeTracker.GetCurrentScope(),
					BlockStmt: parentBlock,
					Index:     i,
					Label:     s.Name,
				})
			}

		case *ast.ProcedureIfInfo:
			// Process IF branches
			pi.processIfInfo(s)

		case *ast.ProcedureLabelLoop:
			// Process labeled loop
			pi.processLabelLoop(s)

		case *ast.ProcedureWhileStmt:
			// Process WHILE body (no new scope, just find LEAVE statements)
			pi.processBlockStatements(parentBlock, s.Body)

		case *ast.ProcedureRepeatStmt:
			// Process REPEAT body
			pi.processBlockStatements(parentBlock, s.Body)
		}
	}
}

// processIfStatements processes statements inside an IF THEN branch
func (pi *ProcedureInserter) processIfStatements(ifBlock *ast.ProcedureIfBlock, stmts []ast.StmtNode) {
	for i, stmt := range stmts {
		switch s := stmt.(type) {
		case *ast.ProcedureLabelBlock:
			pi.ProcessLabeledBlock(s)

		case *ast.ProcedureJump:
			if s.IsLeave {
				pi.insertPoints = append(pi.insertPoints, InsertionPoint{
					Type:    InsertBeforeLeave,
					Scope:   pi.scopeTracker.GetCurrentScope(),
					IfBlock: ifBlock,
					Index:   i,
					Label:   s.Name,
				})
			}

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
	for i, stmt := range stmts {
		switch s := stmt.(type) {
		case *ast.ProcedureLabelBlock:
			pi.ProcessLabeledBlock(s)

		case *ast.ProcedureJump:
			if s.IsLeave {
				pi.insertPoints = append(pi.insertPoints, InsertionPoint{
					Type:      InsertBeforeLeave,
					Scope:     pi.scopeTracker.GetCurrentScope(),
					ElseBlock: elseBlock,
					Index:     i,
					Label:     s.Name,
				})
			}

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

		case *ast.ProcedureIfInfo:
			pi.processIfInfo(s)

		case *ast.ProcedureLabelLoop:
			pi.processLabelLoop(s)

		case *ast.ProcedureWhileStmt:
			pi.processLoopBody(s.Body)

		case *ast.ProcedureRepeatStmt:
			pi.processLoopBody(s.Body)

		// Note: We don't add insertion points for LEAVE inside loops here
		// because they typically exit the loop, not the procedure
		// The LEAVE statements targeting procedure labels are handled
		// in processIfStatements/processElseStatements
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
