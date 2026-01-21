package parser

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"

	pb "parserlib/pb"
)

// processStmtNode processes a statement node and returns a Query protobuf message.
func processStmtNode(stmt *ast.StmtNode) *pb.Query {
	if *stmt == nil {
		return nil
	}

	if isDMLNode(stmt) {
		query := &pb.DMLQuery{}
		processDMLNode(query, stmt)
		return &pb.Query{
			Type: pb.Query_DML,
			Dml:  query,
		}
	} else if isSetNode(stmt) {
		setQuery := processSetStmt((*stmt).(*ast.SetStmt))
		return &pb.Query{
			Type: pb.Query_SET,
			Set:  setQuery,
		}
	} else if isProcInfo(stmt) {
		procedure := &pb.Procedure{}
		processProcInfo(procedure, (*stmt).(*ast.ProcedureInfo))
		return &pb.Query{
			Type:      pb.Query_PROCEDURE,
			Procedure: procedure,
		}
	} else if isProcNode(stmt) {
		return processProcNode(stmt)
	}

	fmt.Printf("FIXME: Unsupported statement type: %v\n", reflect.TypeOf(*stmt))
	return nil
}

// isSetNode checks if the node is a SET statement.
func isSetNode(node *ast.StmtNode) bool {
	_, ok := (*node).(*ast.SetStmt)
	return ok
}

// processSetStmt processes a SET statement and returns a SetQuery protobuf message.
func processSetStmt(stmt *ast.SetStmt) *pb.SetQuery {
	setQuery := &pb.SetQuery{
		Assignments: make([]*pb.SetVariable, len(stmt.Variables)),
	}

	for i, v := range stmt.Variables {
		setVar := &pb.SetVariable{
			Name:     v.Name,
			IsGlobal: v.IsGlobal,
			IsSystem: v.IsSystem,
		}

		if v.Value != nil {
			setVar.Value = processExprNode(&v.Value)
		}

		setQuery.Assignments[i] = setVar
	}

	return setQuery
}

// isDMLNode checks if the node is a DML statement.
func isDMLNode(node *ast.StmtNode) bool {
	switch (*node).(type) {
	case *ast.SelectStmt, *ast.InsertStmt, *ast.UpdateStmt, *ast.DeleteStmt:
		return true
	default:
		return false
	}
}

// processDMLNode fills the DMLQuery protobuf message from a DML statement.
func processDMLNode(query *pb.DMLQuery, node *ast.StmtNode) {
	query.Statement = reprStmtNode(node)

	switch stmt := (*node).(type) {
	case *ast.SelectStmt:
		processSelectStmt(query, stmt)
	case *ast.InsertStmt:
		processInsertStmt(query, stmt)
	case *ast.UpdateStmt:
		processUpdateStmt(query, stmt)
	case *ast.DeleteStmt:
		processDeleteStmt(query, stmt)
	default:
		fmt.Printf("processDMLNode: Unsupported statement type: %v\n", node)
	}
}

// reprStmtNode reconstructs the SQL string from a statement node.
func reprStmtNode(node *ast.StmtNode) string {
	var sb strings.Builder
	err := (*node).Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	if err != nil {
		return ""
	}
	return sb.String()
}

// processSelectStmt processes a SELECT statement.
func processSelectStmt(query *pb.DMLQuery, stmt *ast.SelectStmt) {
	query.Type = pb.DMLQuery_SELECT

	if stmt.From != nil {
		tableRefs := stmt.From.TableRefs
		primaryTable, joinedTables := selectGetTables(tableRefs)

		if primaryTable != nil {
			query.Table = &pb.AliasedIdentifier{
				Alias: primaryTable.Name.O,
				Real: &pb.DMLQueryExpr{
					Operator:   pb.DMLQueryExpr_VALUE,
					ValueType:  pb.DMLQueryExpr_IDENTIFIER,
					Identifier: primaryTable.Name.O,
				},
			}
		}

		query.Join = make([]*pb.AliasedIdentifier, len(joinedTables))
		for i, joinedTable := range joinedTables {
			query.Join[i] = &pb.AliasedIdentifier{
				Alias: joinedTable.Name.O,
				Real: &pb.DMLQueryExpr{
					Operator:   pb.DMLQueryExpr_VALUE,
					ValueType:  pb.DMLQueryExpr_IDENTIFIER,
					Identifier: joinedTable.Name.O,
				},
			}
		}
	}

	if stmt.Where != nil {
		query.Where = processExprNode(&stmt.Where)
	}

	query.Select = make([]*pb.AliasedIdentifier, len(stmt.Fields.Fields))
	for i, field := range stmt.Fields.Fields {
		query.Select[i] = &pb.AliasedIdentifier{
			Alias: field.AsName.O,
			Real:  processExprNode(&field.Expr),
		}
	}

	// Handle SELECT INTO variables
	if stmt.SelectIntoOpt != nil && stmt.SelectIntoOpt.Tp == ast.SelectIntoVars {
		query.IntoVariables = make([]string, len(stmt.SelectIntoOpt.Variables))
		for i, v := range stmt.SelectIntoOpt.Variables {
			if v.ColumnName != nil {
				query.IntoVariables[i] = v.ColumnName.Name.O
			} else if v.UserVar != nil {
				query.IntoVariables[i] = v.UserVar.Name
			}
		}
	}
}

// selectGetTables extracts the primary table and joined tables from a JOIN clause.
func selectGetTables(tableRefs *ast.Join) (*ast.TableName, []*ast.TableName) {
	var primaryTable *ast.TableName
	var joinedTables []*ast.TableName

	if tableRefs.Left != nil {
		switch left := tableRefs.Left.(type) {
		case *ast.Join:
			primaryTable, joinedTables = selectGetTables(left)
		case *ast.TableSource:
			if tn, ok := left.Source.(*ast.TableName); ok {
				primaryTable = tn
			}
		}
	}

	if tableRefs.Right != nil {
		switch right := tableRefs.Right.(type) {
		case *ast.Join:
			_, rightJoined := selectGetTables(right)
			joinedTables = append(joinedTables, rightJoined...)
		case *ast.TableSource:
			if tn, ok := right.Source.(*ast.TableName); ok {
				joinedTables = append(joinedTables, tn)
			}
		}
	}

	return primaryTable, joinedTables
}

// processInsertStmt processes an INSERT statement.
func processInsertStmt(query *pb.DMLQuery, stmt *ast.InsertStmt) {
	query.Type = pb.DMLQuery_INSERT

	tableName := stmt.Table.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).Name.O
	query.Table = &pb.AliasedIdentifier{
		Alias: tableName,
		Real: &pb.DMLQueryExpr{
			Operator:   pb.DMLQueryExpr_VALUE,
			ValueType:  pb.DMLQueryExpr_IDENTIFIER,
			Identifier: tableName,
		},
	}

	if len(stmt.Lists) == 0 {
		fmt.Printf("processInsertStmt(): column definition is empty: %s\n", query.Statement)
		return
	}

	query.UpdateOrWrite = make([]*pb.DMLQueryExpr, len(stmt.Lists[0]))

	if len(stmt.Columns) != len(stmt.Lists[0]) {
		fmt.Printf("processInsertStmt(): column definition and value count mismatch: %s\n", query.Statement)
		return
	}

	for i, expr := range stmt.Lists[0] {
		columnDef := stmt.Columns[i]
		query.UpdateOrWrite[i] = &pb.DMLQueryExpr{
			Operator: pb.DMLQueryExpr_EQ,
			Right:    processExprNode(&expr),
		}
		if columnDef != nil {
			query.UpdateOrWrite[i].Left = &pb.DMLQueryExpr{
				Operator:   pb.DMLQueryExpr_VALUE,
				ValueType:  pb.DMLQueryExpr_IDENTIFIER,
				Identifier: columnDef.Name.O,
			}
		}
	}
}

// processUpdateStmt processes an UPDATE statement.
func processUpdateStmt(query *pb.DMLQuery, stmt *ast.UpdateStmt) {
	query.Type = pb.DMLQuery_UPDATE

	tableName := stmt.TableRefs.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).Name.O
	query.Table = &pb.AliasedIdentifier{
		Alias: tableName,
		Real: &pb.DMLQueryExpr{
			Operator:   pb.DMLQueryExpr_VALUE,
			ValueType:  pb.DMLQueryExpr_IDENTIFIER,
			Identifier: tableName,
		},
	}

	query.Where = processExprNode(&stmt.Where)
	query.UpdateOrWrite = make([]*pb.DMLQueryExpr, len(stmt.List))

	for i, assignment := range stmt.List {
		query.UpdateOrWrite[i] = &pb.DMLQueryExpr{
			Operator: pb.DMLQueryExpr_EQ,
			Left: &pb.DMLQueryExpr{
				Operator:   pb.DMLQueryExpr_VALUE,
				ValueType:  pb.DMLQueryExpr_IDENTIFIER,
				Identifier: assignment.Column.Name.O,
			},
			Right: processExprNode(&assignment.Expr),
		}
	}
}

// processDeleteStmt processes a DELETE statement.
func processDeleteStmt(query *pb.DMLQuery, stmt *ast.DeleteStmt) {
	query.Type = pb.DMLQuery_DELETE

	tableName := stmt.TableRefs.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).Name.O
	query.Table = &pb.AliasedIdentifier{
		Alias: tableName,
		Real: &pb.DMLQueryExpr{
			Operator:   pb.DMLQueryExpr_VALUE,
			ValueType:  pb.DMLQueryExpr_IDENTIFIER,
			Identifier: tableName,
		},
	}

	query.Where = processExprNode(&stmt.Where)
}
