package main

import "C"
import (
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/opcode"
	types2 "github.com/pingcap/tidb/types"
	_ "github.com/pingcap/tidb/types/parser_driver"
	pb "parserlib/pb"
)

var parser_instances map[int64]*parser.Parser

func get_parser_instance_for(threadId int64) *parser.Parser {
	// returns a parser instance for the given threadId

	if parser_instances[threadId] == nil {
		parser_instances[threadId] = parser.New()
	}

	return parser_instances[threadId]
}

func parse_sql(sql string, threadId int64) (*ast.StmtNode, []error, error) {
	p := get_parser_instance_for(threadId)
	stmtNodes, warns, err := p.Parse(sql, "", "")

	if err != nil {
		return nil, warns, err
	}

	return &stmtNodes[0], warns, nil
}

func protobuf_to_cstr(message proto.Message) (*C.char, int64) {
	// returns a C string representation of the given protobuf message

	data, err := proto.Marshal(message)

	if err != nil {
		fmt.Printf("protobuf_to_cstr: error marshalling protobuf message: %v\n", err.Error())
		return nil, 0
	}

	return C.CString(string(data)), int64(len(data))
}

func process_expr_node(query *pb.DMLQuery, expr *ast.ExprNode) *pb.DMLQueryExpr {
	// processes the where clause of a select statement

	if expr == nil {
		return nil
	}

	if columnNameExpr, ok := (*expr).(*ast.ColumnNameExpr); ok {
		return &pb.DMLQueryExpr{
			Operator:   pb.DMLQueryExpr_VALUE,
			ValueType:  pb.DMLQueryExpr_IDENTIFIER,
			Identifier: columnNameExpr.Name.Name.O,
		}
	} else if valueExpr, ok := (*expr).(ast.ValueExpr); ok {
		tp := valueExpr.GetType().GetType()

		if types2.IsTypeVarchar(tp) {
			return &pb.DMLQueryExpr{
				Operator:  pb.DMLQueryExpr_VALUE,
				ValueType: pb.DMLQueryExpr_STRING,
				String_:   valueExpr.GetValue().(string),
			}
		} else if types2.IsTypeInteger(tp) {
			return &pb.DMLQueryExpr{
				Operator:  pb.DMLQueryExpr_VALUE,
				ValueType: pb.DMLQueryExpr_INTEGER,
				Integer:   valueExpr.GetValue().(int64),
			}
		} else if types2.IsTypeFloat(tp) {
			return &pb.DMLQueryExpr{
				Operator:  pb.DMLQueryExpr_VALUE,
				ValueType: pb.DMLQueryExpr_DOUBLE,
				Double:    valueExpr.GetValue().(float64),
			}
		} else {
			return &pb.DMLQueryExpr{
				Operator:  pb.DMLQueryExpr_VALUE,
				ValueType: pb.DMLQueryExpr_UNKNOWN_VALUE,
				String_:   valueExpr.GetString(),
			}
		}
	} else if functionCallExpr, ok := (*expr).(*ast.FuncCallExpr); ok {
		expr_list := make([]*pb.DMLQueryExpr, len(functionCallExpr.Args))

		for i, arg := range functionCallExpr.Args {
			expr_list[i] = process_expr_node(query, &arg)
		}

		return &pb.DMLQueryExpr{
			Operator:  pb.DMLQueryExpr_VALUE,
			ValueType: pb.DMLQueryExpr_FUNCTION,
			Function:  functionCallExpr.FnName.O,
			ValueList: expr_list,
		}
	} else if binaryExpr, ok := (*expr).(*ast.BinaryOperationExpr); ok {

		var expr_out = pb.DMLQueryExpr{}

		switch binaryExpr.Op {
		case opcode.LT:
			expr_out.Operator = pb.DMLQueryExpr_LT
			break
		case opcode.LE:
			expr_out.Operator = pb.DMLQueryExpr_LTE
			break
		case opcode.GT:
			expr_out.Operator = pb.DMLQueryExpr_GT
			break
		case opcode.GE:
			expr_out.Operator = pb.DMLQueryExpr_GTE
			break
		case opcode.EQ:
			expr_out.Operator = pb.DMLQueryExpr_EQ
			break
		case opcode.NE:
			expr_out.Operator = pb.DMLQueryExpr_NEQ
			break
		case opcode.In:
			expr_out.Operator = pb.DMLQueryExpr_IN
			break
		case opcode.Like:
			expr_out.Operator = pb.DMLQueryExpr_LIKE
			break
		case opcode.IsNull:
			expr_out.Operator = pb.DMLQueryExpr_IS_NULL
			break
		case opcode.LogicAnd:
			expr_out.Operator = pb.DMLQueryExpr_AND
			break
		case opcode.LogicOr:
			expr_out.Operator = pb.DMLQueryExpr_OR
			break
		case opcode.Plus:
			expr_out.Operator = pb.DMLQueryExpr_PLUS
			break
		case opcode.Minus:
			expr_out.Operator = pb.DMLQueryExpr_MINUS
			break
		case opcode.Mul:
			expr_out.Operator = pb.DMLQueryExpr_MUL
			break
		case opcode.Div:
			expr_out.Operator = pb.DMLQueryExpr_DIV
			break
		case opcode.Mod:
			expr_out.Operator = pb.DMLQueryExpr_MOD
			break
		default:
			fmt.Printf("FIXME: Unsupported binary operator: %s\n", binaryExpr.Op.String())
			break
		}

		if (binaryExpr.Op == opcode.LogicAnd) || (binaryExpr.Op == opcode.LogicOr) {
			expr_out.Expressions = make([]*pb.DMLQueryExpr, 2)
			expr_out.Expressions[0] = process_expr_node(query, &binaryExpr.L)
			expr_out.Expressions[1] = process_expr_node(query, &binaryExpr.R)
		} else {
			expr_out.Left = process_expr_node(query, &binaryExpr.L)
			expr_out.Right = process_expr_node(query, &binaryExpr.R)
		}

		return &expr_out
	} else if parenExpr, ok := (*expr).(*ast.ParenthesesExpr); ok {
		return process_expr_node(query, &parenExpr.Expr)
	} else {
		fmt.Printf("FIXME: Unsupported expression type: %T\n", *expr)

		return &pb.DMLQueryExpr{
			Operator:  pb.DMLQueryExpr_UNKNOWN,
			ValueType: pb.DMLQueryExpr_UNKNOWN_VALUE,
		}
	}

	return nil
}

func process_select_stmt(query *pb.DMLQuery, stmt *ast.SelectStmt) {
	query.Type = pb.DMLQuery_SELECT
	// FIXME
	query.Table = &pb.AliasedIdentifier{
		Alias: (*stmt).From.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).Name.O,
		Real: &pb.DMLQueryExpr{
			Operator:   pb.DMLQueryExpr_VALUE,
			ValueType:  pb.DMLQueryExpr_IDENTIFIER,
			Identifier: (*stmt).From.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).Name.O,
		},
	}
	query.Where = process_expr_node(query, &stmt.Where)
	query.Select = make([]*pb.AliasedIdentifier, len(stmt.Fields.Fields))

	for i, field := range stmt.Fields.Fields {
		query.Select[i] = &pb.AliasedIdentifier{
			Alias: field.AsName.O,
			Real:  process_expr_node(query, &field.Expr),
		}
	}
}

func process_insert_stmt(query *pb.DMLQuery, stmt *ast.InsertStmt) {
	query.Type = pb.DMLQuery_INSERT
	// FIXME
	query.Table = &pb.AliasedIdentifier{
		Alias: (*stmt).Table.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).Name.O,
		Real: &pb.DMLQueryExpr{
			Operator:   pb.DMLQueryExpr_VALUE,
			ValueType:  pb.DMLQueryExpr_IDENTIFIER,
			Identifier: (*stmt).Table.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).Name.O,
		},
	}

	query.UpdateOrWrite = make([]*pb.DMLQueryExpr, len(stmt.Lists[0]))

	for i, expr := range stmt.Lists[0] {
		columnDef := stmt.Columns[i]

		query.UpdateOrWrite[i] = &pb.DMLQueryExpr{
			Operator: pb.DMLQueryExpr_EQ,
			Right:    process_expr_node(query, &expr),
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

func process_update_stmt(query *pb.DMLQuery, stmt *ast.UpdateStmt) {
	query.Type = pb.DMLQuery_UPDATE
	// FIXME
	query.Table = &pb.AliasedIdentifier{
		Alias: (*stmt).TableRefs.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).Name.O,
		Real: &pb.DMLQueryExpr{
			Operator:   pb.DMLQueryExpr_VALUE,
			ValueType:  pb.DMLQueryExpr_IDENTIFIER,
			Identifier: (*stmt).TableRefs.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).Name.O,
		},
	}

	query.Where = process_expr_node(query, &stmt.Where)
	query.UpdateOrWrite = make([]*pb.DMLQueryExpr, len(stmt.List))

	for i, assignment := range stmt.List {
		query.UpdateOrWrite[i] = &pb.DMLQueryExpr{
			Operator: pb.DMLQueryExpr_EQ,
			Left: &pb.DMLQueryExpr{
				Operator:   pb.DMLQueryExpr_VALUE,
				ValueType:  pb.DMLQueryExpr_IDENTIFIER,
				Identifier: assignment.Column.Name.O,
			},
			Right: process_expr_node(query, &assignment.Expr),
		}
	}
}

func process_delete_stmt(query *pb.DMLQuery, stmt *ast.DeleteStmt) {
	query.Type = pb.DMLQuery_DELETE
	// FIXME
	query.Table = &pb.AliasedIdentifier{
		Alias: (*stmt).TableRefs.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).Name.O,
		Real: &pb.DMLQueryExpr{
			Operator:   pb.DMLQueryExpr_VALUE,
			ValueType:  pb.DMLQueryExpr_IDENTIFIER,
			Identifier: (*stmt).TableRefs.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).Name.O,
		},
	}

	query.Where = process_expr_node(query, &stmt.Where)
}

func process_node(query *pb.DMLQuery, node *ast.StmtNode) {
	if selectStmt, ok := (*node).(*ast.SelectStmt); ok {
		process_select_stmt(query, selectStmt)
	} else if insertStmt, ok := (*node).(*ast.InsertStmt); ok {
		process_insert_stmt(query, insertStmt)
	} else if updateStmt, ok := (*node).(*ast.UpdateStmt); ok {
		process_update_stmt(query, updateStmt)
	} else if deleteStmt, ok := (*node).(*ast.DeleteStmt); ok {
		process_delete_stmt(query, deleteStmt)
	} else {
		fmt.Printf("FIXME: Unsupported statement type: %T\n", node)
	}
}

//export ult_sql_parser_init
func ult_sql_parser_init() {
	if parser_instances == nil {
		parser_instances = make(map[int64]*parser.Parser)
	}
}

//export ult_sql_parser_deinit
func ult_sql_parser_deinit() {
	parser_instances = nil
}

//export ult_sql_parse
func ult_sql_parse(sql_cstr *C.char, threadId int64, output **C.char) int64 {
	var result = pb.ParseResult{
		Result: pb.ParseResult_UNKNOWN,
	}
	var size int64

	sql := C.GoString(sql_cstr)

	ast_node, warns, err := parse_sql(sql, threadId)

	if err != nil {
		result.Result = pb.ParseResult_ERROR
		result.Error = err.Error()
		result.Warnings = make([]string, len(warns))

		for i, warn := range warns {
			result.Warnings[i] = warn.Error()
		}

		*output, size = protobuf_to_cstr(&result)

		return size
	}

	result.Result = pb.ParseResult_SUCCESS
	result.Warnings = make([]string, len(warns))

	for i, warn := range warns {
		result.Warnings[i] = warn.Error()
	}

	var query = pb.DMLQuery{}

	result.Dml = &query
	process_node(&query, ast_node)

	*output, size = protobuf_to_cstr(&result)

	return size
}

//export ult_map_insert
func ult_map_insert(stmt *C.char) *C.char {
	return nil
}

func main() {
	// This is a dummy function to make sure that the package compiles.
}
