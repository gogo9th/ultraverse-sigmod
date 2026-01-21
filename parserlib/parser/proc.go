package parser

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/parser/ast"

	pb "parserlib/pb"
)

// isProcInfo checks if the node is a procedure definition.
func isProcInfo(node *ast.StmtNode) bool {
	_, ok := (*node).(*ast.ProcedureInfo)
	return ok
}

// isProcNode checks if the node is a procedure-related node.
func isProcNode(node *ast.StmtNode) bool {
	switch (*node).(type) {
	case *ast.ProcedureIfInfo, *ast.ProcedureIfBlock, *ast.ProcedureLabelLoop,
		*ast.ProcedureLabelBlock, *ast.ProcedureElseIfBlock, *ast.ProcedureElseBlock,
		*ast.ProcedureJump:
		return true
	default:
		return false
	}
}

// processProcInfo processes a procedure definition and fills the Procedure protobuf message.
func processProcInfo(procedure *pb.Procedure, node *ast.ProcedureInfo) {
	procedure.Name = node.ProcedureName.Name.O
	procedure.Parameters = make([]*pb.ProcedureVariable, len(node.ProcedureParam))

	for i, param := range node.ProcedureParam {
		procedure.Parameters[i] = &pb.ProcedureVariable{
			Name:         param.ParamName,
			Type:         param.ParamType.String(),
			DefaultValue: nil,
		}
	}

	// Extract the block from either ProcedureLabelBlock or ProcedureBlock
	var block *ast.ProcedureBlock
	if labelStmt, ok := (node.ProcedureBody).(*ast.ProcedureLabelBlock); ok {
		block = labelStmt.Block
	} else if directBlock, ok := (node.ProcedureBody).(*ast.ProcedureBlock); ok {
		block = directBlock
	} else if node.ProcedureBody != nil {
		fmt.Printf("FIXME: Unsupported procedure body type: %T\n", node.ProcedureBody)
		return
	}

	if block != nil {
		for _, variable := range block.ProcedureVars {
			if vardecl, ok := variable.(*ast.ProcedureDecl); ok {
				for _, declName := range vardecl.DeclNames {
					procedure.Variables = append(procedure.Variables, &pb.ProcedureVariable{
						Name:         declName,
						Type:         (*vardecl.DeclType).String(),
						DefaultValue: processExprNode(&vardecl.DeclDefault),
					})
				}
			}
		}

		for _, stmt := range block.ProcedureProcStmts {
			query := processStmtNode(&stmt)
			if query != nil {
				procedure.Statements = append(procedure.Statements, query)
			}
		}
	}
}

// processProcNode processes procedure-related nodes and returns a Query protobuf message.
func processProcNode(node *ast.StmtNode) *pb.Query {
	switch stmt := (*node).(type) {
	case *ast.ProcedureIfInfo:
		ifBlock := processProcIf(stmt.IfBody)
		return &pb.Query{
			Type:    pb.Query_IF,
			IfBlock: ifBlock,
		}

	case *ast.ProcedureLabelLoop:
		whileBlock := processProcLoop(stmt)
		return &pb.Query{
			Type:       pb.Query_WHILE,
			WhileBlock: whileBlock,
		}

	case *ast.ProcedureLabelBlock:
		ifBlock := processProcLabelBlock(stmt)
		return &pb.Query{
			Type:    pb.Query_IF,
			IfBlock: ifBlock,
		}

	case *ast.ProcedureElseIfBlock:
		ifBlock := processProcIf(stmt.ProcedureIfStmt)
		return &pb.Query{
			Type:    pb.Query_IF,
			IfBlock: ifBlock,
		}

	case *ast.ProcedureElseBlock:
		block := &pb.ProcedureIfBlock{}
		for _, s := range stmt.ProcedureIfStmts {
			x := processStmtNode(&s)
			if x != nil {
				block.ThenBlock = append(block.ThenBlock, x)
			}
		}
		return &pb.Query{
			Type:    pb.Query_IF,
			IfBlock: block,
		}

	case *ast.ProcedureJump:
		// do nothing
		return nil

	default:
		fmt.Printf("FIXME: Unsupported procedure type: %T\n", *node)
		return nil
	}
}

// processProcIf processes an IF block in a procedure.
func processProcIf(node *ast.ProcedureIfBlock) *pb.ProcedureIfBlock {
	block := &pb.ProcedureIfBlock{
		Condition: processExprNode(&node.IfExpr),
	}

	for _, stmt := range node.ProcedureIfStmts {
		x := processStmtNode(&stmt)
		if x != nil {
			block.ThenBlock = append(block.ThenBlock, x)
		}
	}

	y := processStmtNode(&node.ProcedureElseStmt)
	if y != nil {
		block.ElseBlock = append(block.ElseBlock, y)
	}

	return block
}

// processProcLoop processes a WHILE loop in a procedure.
func processProcLoop(node *ast.ProcedureLabelLoop) *pb.ProcedureWhileBlock {
	if loop, ok := node.Block.(*ast.ProcedureWhileStmt); ok {
		block := &pb.ProcedureWhileBlock{
			Condition: processExprNode(&loop.Condition),
		}

		for _, stmt := range loop.Body {
			x := processStmtNode(&stmt)
			if x != nil {
				block.Block = append(block.Block, x)
			}
		}

		return block
	}

	fmt.Printf("FIXME: Unsupported procedure loop type: %T\n", node.Block)
	return nil
}

// processProcLabelBlock processes a labeled block in a procedure.
func processProcLabelBlock(node *ast.ProcedureLabelBlock) *pb.ProcedureIfBlock {
	block := &pb.ProcedureIfBlock{}

	for _, stmt := range node.Block.ProcedureProcStmts {
		x := processStmtNode(&stmt)
		if x != nil {
			block.ThenBlock = append(block.ThenBlock, x)
		}
	}

	return block
}
