package main

import "C"
import (
	"encoding/json"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	types2 "github.com/pingcap/tidb/types"
	_ "github.com/pingcap/tidb/types/parser_driver"
	pb "parserlib/pb"
	"reflect"
	"regexp"
	"strings"
	"sync"
)

/**
 * 글로벌 파서 인스턴스
 * tidb의 파서는 thread-safe 하지 않기 때문에 각 스레드마다 파서 인스턴스를 생성해서 사용하고 있습니다.
 * 스레드 ID를 키로 사용합니다.
 */
var parser_instances map[int64]*parser.Parser
var parser_mutex sync.Mutex

/**
 * 스레드 ID에 맞는 파서 인스턴스를 반환합니다.
 */
func get_parser_instance_for(threadId int64) *parser.Parser {
	// returns a parser instance for the given threadId
	parser_mutex.Lock()
	if parser_instances == nil {
		parser_instances = make(map[int64]*parser.Parser)
	}

	if parser_instances[threadId] == nil {
		parser_instances[threadId] = parser.New()
	}

	instance := parser_instances[threadId]
	parser_mutex.Unlock()

	return instance
}

/**
 * tidb.parser 모듈을 사용하여 SQL을 파싱합니다.
 */
func parse_sql(sql string, threadId int64) ([]ast.StmtNode, []error, error) {
	p := get_parser_instance_for(threadId)
	stmtNodes, warns, err := p.Parse(sql, "", "")

	if err != nil {
		return nil, warns, err
	}

	return stmtNodes, warns, nil
}

/**
 * Protobuf 메시지를 C언어에서 사용하는 문자열로 변환합니다.
 * 파싱 결과를 C++ 프로그램에 전달하기 위해 사용합니다.
 * @return (C언어 문자열, 문자열 길이)의 pair
 */
func protobuf_to_cstr(message proto.Message) (*C.char, int64) {
	// returns a C string representation of the given protobuf message

	data, err := proto.Marshal(message)

	if err != nil {
		fmt.Printf("protobuf_to_cstr: error marshalling protobuf message: %v\n", err.Error())
		return nil, 0
	}

	return C.CString(string(data)), int64(len(data))
}

/**
 * Expression 노드를 DMLQueryExpr protobuf 메시지로 변환합니다.
 */
func process_expr_node(expr *ast.ExprNode) *pb.DMLQueryExpr {
	// processes the where clause of a select statement

	if *expr == nil {
		return nil
	}

	if columnNameExpr, ok := (*expr).(*ast.ColumnNameExpr); ok {
		identifier := columnNameExpr.Name.Name.O
		if columnNameExpr.Name.Table.O != "" {
			identifier = columnNameExpr.Name.Table.O + "." + identifier
		}
		return &pb.DMLQueryExpr{
			Operator:   pb.DMLQueryExpr_VALUE,
			ValueType:  pb.DMLQueryExpr_IDENTIFIER,
			Identifier: identifier,
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
		} else if tp == mysql.TypeNewDecimal {
			decimalValue := ""
			if raw := valueExpr.GetValue(); raw != nil {
				if dec, ok := raw.(*types2.MyDecimal); ok {
					decimalValue = dec.String()
				} else {
					decimalValue = fmt.Sprintf("%v", raw)
				}
			}
			return &pb.DMLQueryExpr{
				Operator:  pb.DMLQueryExpr_VALUE,
				ValueType: pb.DMLQueryExpr_DECIMAL,
				Decimal:   decimalValue,
			}
		} else if tp == mysql.TypeDouble || types2.IsTypeFloat(tp) {
			return &pb.DMLQueryExpr{
				Operator:  pb.DMLQueryExpr_VALUE,
				ValueType: pb.DMLQueryExpr_DOUBLE,
				Double:    valueExpr.GetValue().(float64), // valueExpr.GetFloat64(),
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
			expr_list[i] = process_expr_node(&arg)
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
			expr_out.Expressions[0] = process_expr_node(&binaryExpr.L)
			expr_out.Expressions[1] = process_expr_node(&binaryExpr.R)
		} else {
			expr_out.Left = process_expr_node(&binaryExpr.L)
			expr_out.Right = process_expr_node(&binaryExpr.R)
		}

		return &expr_out
	} else if betweenExpr, ok := (*expr).(*ast.BetweenExpr); ok {
		// Expand BETWEEN into AND/OR so downstream logic can handle simple comparisons.
		leftA := process_expr_node(&betweenExpr.Expr)
		leftB := process_expr_node(&betweenExpr.Expr)
		low := process_expr_node(&betweenExpr.Left)
		high := process_expr_node(&betweenExpr.Right)

		if betweenExpr.Not {
			return &pb.DMLQueryExpr{
				Operator: pb.DMLQueryExpr_OR,
				Expressions: []*pb.DMLQueryExpr{
					{
						Operator: pb.DMLQueryExpr_LT,
						Left:     leftA,
						Right:    low,
					},
					{
						Operator: pb.DMLQueryExpr_GT,
						Left:     leftB,
						Right:    high,
					},
				},
			}
		}

		return &pb.DMLQueryExpr{
			Operator: pb.DMLQueryExpr_AND,
			Expressions: []*pb.DMLQueryExpr{
				{
					Operator: pb.DMLQueryExpr_GTE,
					Left:     leftA,
					Right:    low,
				},
				{
					Operator: pb.DMLQueryExpr_LTE,
					Left:     leftB,
					Right:    high,
				},
			},
		}
	} else if likeExpr, ok := (*expr).(*ast.PatternLikeOrIlikeExpr); ok {
		op := pb.DMLQueryExpr_LIKE
		if likeExpr.Not {
			op = pb.DMLQueryExpr_NOT_LIKE
		}
		return &pb.DMLQueryExpr{
			Operator: op,
			Left:     process_expr_node(&likeExpr.Expr),
			Right:    process_expr_node(&likeExpr.Pattern),
		}
	} else if inExpr, ok := (*expr).(*ast.PatternInExpr); ok {
		op := pb.DMLQueryExpr_IN
		if inExpr.Not {
			op = pb.DMLQueryExpr_NOT_IN
		}

		valueList := make([]*pb.DMLQueryExpr, 0, len(inExpr.List))
		for i := range inExpr.List {
			item := inExpr.List[i]
			if child := process_expr_node(&item); child != nil {
				valueList = append(valueList, child)
			}
		}

		if inExpr.Sel != nil {
			// Subquery IN is not supported yet. Keep an empty list to avoid crashes downstream.
			fmt.Printf("FIXME: Unsupported subquery in IN expression\n")
		}

		return &pb.DMLQueryExpr{
			Operator: op,
			Left:     process_expr_node(&inExpr.Expr),
			Right: &pb.DMLQueryExpr{
				Operator:  pb.DMLQueryExpr_VALUE,
				ValueType: pb.DMLQueryExpr_LIST,
				ValueList: valueList,
			},
		}
	} else if parenExpr, ok := (*expr).(*ast.ParenthesesExpr); ok {
		return process_expr_node(&parenExpr.Expr)
	} else if unaryExpr, ok := (*expr).(*ast.UnaryOperationExpr); ok {
		exprNode := process_expr_node(&unaryExpr.V)

		if exprNode.Operator == pb.DMLQueryExpr_VALUE {
			switch unaryExpr.Op {
			case opcode.Plus:
				break
			case opcode.Minus:
				if exprNode.ValueType == pb.DMLQueryExpr_INTEGER {
					exprNode.Integer = -exprNode.Integer
				} else if exprNode.ValueType == pb.DMLQueryExpr_DOUBLE {
					exprNode.Double = -exprNode.Double
				} else if exprNode.ValueType == pb.DMLQueryExpr_DECIMAL {
					if strings.HasPrefix(exprNode.Decimal, "-") {
						exprNode.Decimal = strings.TrimPrefix(exprNode.Decimal, "-")
					} else if exprNode.Decimal != "0" {
						exprNode.Decimal = "-" + exprNode.Decimal
					}
				}
				break
			default:
				// TODO: 다른 연산자 지원도 추가해야 합니다.
				fmt.Printf("FIXME: Unsupported unary operator: %s\n", unaryExpr.Op.String())
				break
			}
		} else {
			fmt.Printf("FIXME: Unsupported unary operator: %s\n", unaryExpr.Op.String())
		}

		return exprNode

	} else {
		fmt.Printf("FIXME: Unsupported expression type: %T\n", *expr)

		return &pb.DMLQueryExpr{
			Operator:  pb.DMLQueryExpr_UNKNOWN,
			ValueType: pb.DMLQueryExpr_UNKNOWN_VALUE,
		}
	}

	return nil
}

/**
 * SELECT 문에서 사용된 테이블 목록을 반환합니다.
 * FIXME: JOIN 문을 엉망 진창으로 처리하고 있습니다.
 */
func select_get_tables(tableRefs *ast.Join) (*ast.TableName, []*ast.TableName) {
	var primary_table *ast.TableName
	var joined_tables []*ast.TableName

	if tableRefs.Left != nil {
		switch tableRefs.Left.(type) {
		case *ast.Join:
			primary_table, joined_tables = select_get_tables(tableRefs.Left.(*ast.Join))
			break
		case *ast.TableSource:
			primary_table = tableRefs.Left.(*ast.TableSource).Source.(*ast.TableName)
			break
		}
	}

	if tableRefs.Right != nil {
		switch tableRefs.Right.(type) {
		case *ast.Join:
			_, joined_tables = select_get_tables(tableRefs.Right.(*ast.Join))
			break
		case *ast.TableSource:
			joined_tables = append(joined_tables, tableRefs.Right.(*ast.TableSource).Source.(*ast.TableName))
			break
		}
	}

	return primary_table, joined_tables
}

/**
 * SELECT 문을 처리하여 DMLQuery protobuf 메시지를 채워 넣습니다.
 */
func process_select_stmt(query *pb.DMLQuery, stmt *ast.SelectStmt) {
	query.Type = pb.DMLQuery_SELECT

	if stmt.From != nil {
		tableRefs := stmt.From.TableRefs
		primary_table, joined_tables := select_get_tables(tableRefs)

		query.Table = &pb.AliasedIdentifier{
			Alias: primary_table.Name.O,
			Real: &pb.DMLQueryExpr{
				Operator:   pb.DMLQueryExpr_VALUE,
				ValueType:  pb.DMLQueryExpr_IDENTIFIER,
				Identifier: primary_table.Name.O,
			},
		}

		query.Join = make([]*pb.AliasedIdentifier, len(joined_tables))

		for i, joined_table := range joined_tables {
			query.Join[i] = &pb.AliasedIdentifier{
				Alias: joined_table.Name.O,
				Real: &pb.DMLQueryExpr{
					Operator:   pb.DMLQueryExpr_VALUE,
					ValueType:  pb.DMLQueryExpr_IDENTIFIER,
					Identifier: joined_table.Name.O,
				},
			}
		}
	}

	if stmt.Where != nil {
		query.Where = process_expr_node(&stmt.Where)
	}

	query.Select = make([]*pb.AliasedIdentifier, len(stmt.Fields.Fields))

	for i, field := range stmt.Fields.Fields {
		query.Select[i] = &pb.AliasedIdentifier{
			Alias: field.AsName.O,
			Real:  process_expr_node(&field.Expr),
		}
	}
}

/**
 * INSERT 문을 처리하여 DMLQuery protobuf 메시지를 채워 넣습니다.
 */
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

	/*
		 	ultparserlib은 현재 컬럼 힌트가 들어간 INSERT 문에 한해서만 파싱이 가능합니다.
				INSERT INTO table_name (column1, column2, column3, ...) VALUES (value1, value2, value3, ...); => OK
				INSERT INTO table_name VALUES (value1, value2, value3, ...); => NOT OK

			INSERT 문의 컬럼 힌트가 없는 경우에는 C++ 프로그램 측에서 ult_map_insert()를 사용해서 컬럼 힌트를 추가한 뒤에 파싱을 진행해야 합니다.
	*/
	if len(stmt.Lists) == 0 {
		fmt.Printf("process_insert_stmt(): column definition is empty: %s\n", query.Statement)
		return
	}

	query.UpdateOrWrite = make([]*pb.DMLQueryExpr, len(stmt.Lists[0]))

	if len(stmt.Columns) != len(stmt.Lists[0]) {
		fmt.Printf("process_insert_stmt(): column definition and value count mismatch: %s\n", query.Statement)
		return
	}

	for i, expr := range stmt.Lists[0] {
		columnDef := stmt.Columns[i]

		query.UpdateOrWrite[i] = &pb.DMLQueryExpr{
			Operator: pb.DMLQueryExpr_EQ,
			Right:    process_expr_node(&expr),
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

/**
 * UPDATE 문을 처리하여 DMLQuery protobuf 메시지를 채워 넣습니다.
 */
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

	query.Where = process_expr_node(&stmt.Where)
	query.UpdateOrWrite = make([]*pb.DMLQueryExpr, len(stmt.List))

	for i, assignment := range stmt.List {
		query.UpdateOrWrite[i] = &pb.DMLQueryExpr{
			Operator: pb.DMLQueryExpr_EQ,
			Left: &pb.DMLQueryExpr{
				Operator:   pb.DMLQueryExpr_VALUE,
				ValueType:  pb.DMLQueryExpr_IDENTIFIER,
				Identifier: assignment.Column.Name.O,
			},
			Right: process_expr_node(&assignment.Expr),
		}
	}
}

/**
 * DELETE 문을 처리하여 DMLQuery protobuf 메시지를 채워 넣습니다.
 */
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

	query.Where = process_expr_node(&stmt.Where)
}

/**
 * 주어진 ast.StmtNode가 DML 문인지 확인합니다.
 */
func is_dml_node(node *ast.StmtNode) bool {
	switch (*node).(type) {
	case *ast.SelectStmt:
		return true
	case *ast.InsertStmt:
		return true
	case *ast.UpdateStmt:
		return true
	case *ast.DeleteStmt:
		return true
	default:
		return false
	}

	return false
}

/**
 * 주어진 ast.StmtNode가 DML 문이라면 DMLQuery protobuf 메시지를 채워 넣습니다.
 */
func process_dml_node(query *pb.DMLQuery, node *ast.StmtNode) {
	query.Statement = repr_node(node)

	if selectStmt, ok := (*node).(*ast.SelectStmt); ok {
		process_select_stmt(query, selectStmt)
	} else if insertStmt, ok := (*node).(*ast.InsertStmt); ok {
		process_insert_stmt(query, insertStmt)
	} else if updateStmt, ok := (*node).(*ast.UpdateStmt); ok {
		process_update_stmt(query, updateStmt)
	} else if deleteStmt, ok := (*node).(*ast.DeleteStmt); ok {
		process_delete_stmt(query, deleteStmt)
	} else {
		fmt.Printf("process_dml_node: Unsupported statement type: %v\n", node)
	}
}

/**
* 주어진 노드가 프로시저 관련 노드인지 확인합니다.
 */
func is_proc_node(node *ast.StmtNode) bool {
	switch (*node).(type) {
	case *ast.ProcedureIfInfo:
		return true
	case *ast.ProcedureIfBlock:
		return true
	case *ast.ProcedureLabelLoop:
		return true
	case *ast.ProcedureLabelBlock:
		return true
	case *ast.ProcedureElseIfBlock:
		return true
	case *ast.ProcedureElseBlock:
		return true
	case *ast.ProcedureJump:
		return true
	default:
		return false
	}

	return false
}

/**
 * 프로시저의 IF 문을 처리하여 ProcedureIfBlock protobuf 메시지를 반환합니다.
 */
func process_proc_if(node *ast.ProcedureIfBlock) *pb.ProcedureIfBlock {
	block := &pb.ProcedureIfBlock{
		Condition: process_expr_node(&node.IfExpr),
	}

	for _, stmt := range node.ProcedureIfStmts {
		x := process_stmt_node(&stmt)

		if x != nil {
			block.ThenBlock = append(block.ThenBlock, x)
		}
	}

	y := process_stmt_node(&node.ProcedureElseStmt)
	if y != nil {
		block.ElseBlock = append(block.ElseBlock, y)
	}

	return block
}

/**
 * 프로시저의 루프 문을 처리하여 ProcedureWhileBlock protobuf 메시지를 반환합니다.
 */
func process_proc_loop(node *ast.ProcedureLabelLoop) *pb.ProcedureWhileBlock {
	if loop, ok := node.Block.(*ast.ProcedureWhileStmt); ok {
		block := &pb.ProcedureWhileBlock{
			Condition: process_expr_node(&loop.Condition),
		}

		for _, stmt := range loop.Body {
			x := process_stmt_node(&stmt)

			if x != nil {
				block.Block = append(block.Block, x)
			}
		}

		return block
	} else {
		fmt.Printf("FIXME: Unsupported procedure loop type: %T\n", node.Block)
	}

	return nil
}

/**
 * 프로시저의 레이블 블록 처리하여 ProcedureIfBlock protobuf 메시지를 반환합니다.
 * @see https://chat.openai.com/share/9ac5a8f4-7055-440e-9812-02926df5ecfe
 */
func process_proc_label_block(node *ast.ProcedureLabelBlock) *pb.ProcedureIfBlock {
	block := &pb.ProcedureIfBlock{}

	for _, stmt := range node.Block.ProcedureProcStmts {
		x := process_stmt_node(&stmt)

		if x != nil {
			block.ThenBlock = append(block.ThenBlock, x)
		}
	}

	return block
}

/**
 * 프로시저 관련 노드를 처리하여 Query protobuf 메시지를 채워 넣습니다.
 * FIXME: 프로시저 내 모든 쿼리가 추출되고 있지 않습니다.
 */
func process_proc_node(node *ast.StmtNode) *pb.Query {
	if ifStmt, ok := (*node).(*ast.ProcedureIfInfo); ok {
		ifBlock := process_proc_if(ifStmt.IfBody)

		return &pb.Query{
			Type:    pb.Query_IF,
			IfBlock: ifBlock,
		}
	} else if loopStmt, ok := (*node).(*ast.ProcedureLabelLoop); ok {
		whileBlock := process_proc_loop(loopStmt)

		return &pb.Query{
			Type:       pb.Query_WHILE,
			WhileBlock: whileBlock,
		}
	} else if labelBlockStmt, ok := (*node).(*ast.ProcedureLabelBlock); ok {
		// FIXME: 모든 블럭을 새로운 pb.Query_IF로 처리하고 있습니다.
		//        protobuf 메시지 정의에서 블럭 타입을 추가하거나, 프로시저 내 모든 쿼리를 flat하게 저장하도록 수정해야 합니다. (빠른 처리를 위해 개인적으로 후자를 권합니다)
		ifBlock := process_proc_label_block(labelBlockStmt)

		return &pb.Query{
			Type:    pb.Query_IF,
			IfBlock: ifBlock,
		}
	} else if elseIfBlockStmt, ok := (*node).(*ast.ProcedureElseIfBlock); ok {
		// FIXME: 모든 블럭을 새로운 pb.Query_IF로 처리하고 있습니다.
		//        protobuf 메시지 정의에서 블럭 타입을 추가하거나, 프로시저 내 모든 쿼리를 flat하게 저장하도록 수정해야 합니다. (빠른 처리를 위해 개인적으로 후자를 권합니다)
		ifBlock := process_proc_if(elseIfBlockStmt.ProcedureIfStmt)

		return &pb.Query{
			Type:    pb.Query_IF,
			IfBlock: ifBlock,
		}
	} else if elseBlockStmt, ok := (*node).(*ast.ProcedureElseBlock); ok {
		// FIXME: 모든 블럭을 새로운 pb.Query_IF로 처리하고 있습니다.
		//        protobuf 메시지 정의에서 블럭 타입을 추가하거나, 프로시저 내 모든 쿼리를 flat하게 저장하도록 수정해야 합니다. (빠른 처리를 위해 개인적으로 후자를 권합니다)
		block := &pb.ProcedureIfBlock{}

		for _, stmt := range elseBlockStmt.ProcedureIfStmts {
			x := process_stmt_node(&stmt)

			if x != nil {
				block.ThenBlock = append(block.ThenBlock, x)
			}
		}

		return &pb.Query{
			Type:    pb.Query_IF,
			IfBlock: block,
		}
	} else if _, ok := (*node).(*ast.ProcedureJump); ok {
		// do nothing
	} else {
		fmt.Printf("FIXME: Unsupported procedure type: %T\n", node)
	}

	return nil
}

/**
 * 주어진 노드가 프로시저를 *정의*하는 노드인지 확인합니다.
 */
func is_proc_info(node *ast.StmtNode) bool {
	switch (*node).(type) {
	case *ast.ProcedureInfo:
		return true
	default:
		return false
	}

	return false
}

/**
 * 프로시저 정의 노드를 처리하여 Procedure protobuf 메시지를 채워 넣습니다.
 */
func process_proc_info(procedure *pb.Procedure, node *ast.ProcedureInfo) {
	procedure.Name = node.ProcedureName.Name.O
	procedure.Parameters = make([]*pb.ProcedureVariable, len(node.ProcedureParam))

	for i, param := range node.ProcedureParam {
		procedure.Parameters[i] = &pb.ProcedureVariable{
			Name:         param.ParamName,
			Type:         param.ParamType.String(),
			DefaultValue: nil,
		}
	}

	if labelStmt, ok := (node.ProcedureBody).(*ast.ProcedureLabelBlock); ok {
		block := labelStmt.Block

		for _, variable := range block.ProcedureVars {
			if vardecl, ok := variable.(*ast.ProcedureDecl); ok {
				for _, declName := range vardecl.DeclNames {
					procedure.Variables = append(procedure.Variables, &pb.ProcedureVariable{
						Name:         declName,
						Type:         (*vardecl.DeclType).String(),
						DefaultValue: process_expr_node(&vardecl.DeclDefault),
					})
				}
			}
		}

		for _, stmt := range block.ProcedureProcStmts {
			query := process_stmt_node(&stmt)

			if query != nil {
				procedure.Statements = append(procedure.Statements, query)
			}
		}
	}
}

/**
 * statement 노드를 처리하여 Query protobuf 메시지를 채워 넣습니다.
 */
func process_stmt_node(stmt *ast.StmtNode) *pb.Query {
	if *stmt == nil {
		return nil
	}

	if is_dml_node(stmt) {
		query := &pb.DMLQuery{}
		process_dml_node(query, stmt)

		return &pb.Query{
			Type: pb.Query_DML,
			Dml:  query,
		}
	} else if is_proc_info(stmt) {
		procedure := &pb.Procedure{}
		process_proc_info(procedure, (*stmt).(*ast.ProcedureInfo))

		return &pb.Query{
			Type:      pb.Query_PROCEDURE,
			Procedure: procedure,
		}
	} else if is_proc_node(stmt) {
		return process_proc_node(stmt)
	} else {
		fmt.Printf("FIXME: Unsupported statement type: %v\n", reflect.TypeOf(*stmt))
	}

	return nil
}

/**
 * 주어진 노드를 SQL string으로 재구성합니다
 */
func repr_node(node *ast.StmtNode) string {
	var sbuilder strings.Builder

	err := (*node).Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sbuilder))

	if err != nil {
		return ""
	}

	return sbuilder.String()
}

/**
 * 파서를 초기화합니다. (C에서 호출 가능하도록 export 되어 있습니다)
 * C++ 프로그램에서 사용 전 반드시 호출하여 주십시오.
 */
//export ult_sql_parser_init
func ult_sql_parser_init() {
	parser_mutex.Lock()
	if parser_instances == nil {
		parser_instances = make(map[int64]*parser.Parser)
	}
	parser_mutex.Unlock()
}

/**
 * 파서 관련 자원을 해제합니다. (C에서 호출 가능하도록 export 되어 있습니다)
 * C++ 프로그램에서 파서 관련 기능 사용 완료 후 반드시 호출하여 주십시오.
 */
//export ult_sql_parser_deinit
func ult_sql_parser_deinit() {
	parser_mutex.Lock()
	parser_instances = nil
	parser_mutex.Unlock()
}

/**
 * C 프로그램에서 SQL을 파싱합니다.
 * 파싱 결과는 protobuf 메시지로 반환됩니다.
 * @param sql_cstr 파싱할 SQL 문자열
 * @param threadId 스레드 ID
 * @param output 파싱 결과가 저장될 포인터
 * @returns 메시지 크기
 */
//export ult_sql_parse
func ult_sql_parse(sql_cstr *C.char, threadId int64, output **C.char) int64 {
	var result = pb.ParseResult{
		Result: pb.ParseResult_UNKNOWN,
	}
	var size int64

	sql := C.GoString(sql_cstr)

	ast_nodes, warns, err := parse_sql(sql, threadId)

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

	for _, ast_node := range ast_nodes {
		query := process_stmt_node(&ast_node)

		if query != nil {
			result.Statements = append(result.Statements, query)
		}
	}

	*output, size = protobuf_to_cstr(&result)

	return size
}

/**
 * 컬럼 힌트가 없는 INSERT 구문에 힌트를 삽입합니다.
 * TODO: 구현되어 있지 않습니다.
 */
//export ult_map_insert
func ult_map_insert(stmt *C.char) *C.char {
	return nil
}

/**
 * 주어진 쿼리 A와 B가 같은 쿼리인지 비교합니다.
 * 이 function은 아래 두 쿼리를 같은 쿼리로 판정합니다.
 *   SELECT * FROM T1 WHERE A = 1
 *   SELECT    * FROM t1 WHERE       a=1
 */
//export ult_query_match
func ult_query_match(a *C.char, b *C.char, threadId int64) bool {
	sql_a := C.GoString(a)
	sql_b := C.GoString(b)

	ast_a, _, err := parse_sql(sql_a, threadId)

	if err != nil {
		return false
	}

	return repr_node(&ast_a[0]) == sql_b
}

//export ult_normalize_procedure_code
func ult_normalize_procedure_code(procedure_code_cstr *C.char) *C.char {
	// @copilot: please translate the following (TypeScript + Perl)-like pseudocode to Go
	/*
					return procedure_code.replace(/\n/g, ' ')
		                                 .replace(/;/g, ';\n')
			                             .replace(/END/g, "\nEND");
	*/

	procedure_code := C.GoString(procedure_code_cstr)

	replace_newline := regexp.MustCompile(`\n`)
	replace_semicolon := regexp.MustCompile(`;`)
	replace_end := regexp.MustCompile(`END`)

	normalized_procedure_code := replace_end.ReplaceAllString(replace_semicolon.ReplaceAllString(replace_newline.ReplaceAllString(procedure_code, " "), ";\n"), "\nEND")

	return C.CString(normalized_procedure_code)
}

/**
 * SQL 파서로 사용하는 tidb.parser는 프로시저에서 사용하는 변수 할당 구문 (SELECT col INTO var)을 지원하지 않습니다.
 * 이를 억지로 지원하기 위해, SELECT INTO에서 INTO 부분을 제거합니다.
 *   -- 사용자 1의 게임 점수를 읽어서 game_score 변수에 저장한다. 아래 구문은 MySQL에서 지원하고 있지만, 파서에서는 에러가 발생한다.
 *   SELECT game_records.score INTO game_score FROM game_records WHERE game_records.user_id = 1;
 */
//export ult_sanitize_select_into
func ult_sanitize_select_into(query_cstr *C.char) *C.char {
	query := C.GoString(query_cstr)

	regex := regexp.MustCompile(`SELECT\s+(.+?)\s+INTO\s+(.+?)(\s+FROM|\s*;)`)
	// @copilot please translate the following (TypeScript + Perl)-like pseudocode to Go
	// return query.replace(regex, 'SELECT $1 $3');

	sanitized_query := regex.ReplaceAllString(query, "SELECT $1 $3")
	// Q: ?? really? is $1 works?
	// A: yes, it works. $1 is a special variable in Go regexp package. It's a string that contains the text of the leftmost submatch.

	return C.CString(sanitized_query)
}

/**
 * SQL 파서로 사용하는 tidb.parser는 프로시저에서 사용하는 변수 할당 구문 (SELECT col INTO var)을 지원하지 않습니다.
 * 관련 부분을 지원하기 위해 SELECT INTO에서 변수 할당 부분을 추출합니다.
 */
func extract_select_into(procedure_code string) []map[string]string {
	// @copilot please translate the following (TypeScript + Perl)-like pseudocode to Go
	/*
		    const normalizeColumn = (column: string): string => column.replace(/`/g, '').trim();
			const assigns: Record<string, string> = {};
			const regex: RegExp = m/SELECT\s+(?<columns>.+?)\s+INTO\s+(?<vars>.+?)(\s+FROM|\s*;)/i;
			while (query =~ regex) {
				const columns = $+{columns};
				const vars = $+{vars};

				if (columns.length != vars.length) {
			        if (vars.length !== 1) {
		                console.error("Invalid number of variables");
		                return {};
		            }

		            const varFirst = vars[0];
		            columns.map { normalizeColumn(it) }.forEach { assigns[it] = varFirst };
				} else {
		            columns.map { normalizeColumn(it) }.forEachIndexed { i, it -> assigns[it] = vars[i] };
		        }
			}

			return assigns;
	*/

	normalizeColumn := func(column string) string {
		return regexp.MustCompile("`").ReplaceAllString(column, "")
	}

	assign_list := make([]map[string]string, 0)

	regex := regexp.MustCompile(`SELECT\s+(.+?)\s+INTO\s+(.+?)(\s+FROM|\s*;)`)

	// split by semicolon
	queries := strings.Split(procedure_code, ";")

	for _, query := range queries {

		if regexp.MustCompile(`^\s*$`).MatchString(query) {
			continue
		}

		assigns := make(map[string]string)

		if matches := regex.FindStringSubmatch(query); len(matches) > 0 {
			columns := strings.Split(matches[1], ",")
			vars := strings.Split(matches[2], ",")

			if len(columns) != len(vars) {
				if len(vars) != 1 {
					fmt.Printf("Invalid number of variables")

					assign_list = append(assign_list, assigns)
					continue
				}

				varFirst := vars[0]
				for _, column := range columns {
					assigns[normalizeColumn(column)] = varFirst
				}
			} else {
				for i, column := range columns {
					assigns[normalizeColumn(column)] = vars[i]
				}
			}
		}

		assign_list = append(assign_list, assigns)
	}

	return assign_list
}

/**
 * 위 extract_select_into를 C 프로그램에서 사용할 수 있도록 래핑합니다.
 */
//export ult_extract_select_info
func ult_extract_select_info(procedure_code_cstr *C.char, output **C.char) int64 {
	procedure_code := C.GoString(procedure_code_cstr)
	assign_list := extract_select_into(procedure_code)

	var size int64 = 0

	result := &pb.SelectIntoExtractionResult{}

	for _, assign_map := range assign_list {
		result.Results = append(result.Results, &pb.SelectIntoAssignmentMap{
			Assignments: assign_map,
		})
	}

	*output, size = protobuf_to_cstr(result)

	return size
}

/**
 * tidb에서 파싱한 AST를 JSON으로 변환합니다.
 */
//export ult_parse_jsonify
func ult_parse_jsonify(sql_cstr *C.char, output **C.char, threadId int64) int64 {
	sql := C.GoString(sql_cstr)

	ast_nodes, _, err := parse_sql(sql, threadId)

	if err != nil {
		*output = nil
		return 0
	}

	ast_node := ast_nodes[0]

	b, _ := json.Marshal(ast_node)

	*output = C.CString(string(b))

	return int64(len(b))
}

func main() {
	// This is a dummy function to make sure that the package compiles.
}
