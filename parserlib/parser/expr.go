package parser

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/types"

	pb "parserlib/pb"
)

// processExprNode converts an AST expression node to a DMLQueryExpr protobuf message.
func processExprNode(expr *ast.ExprNode) *pb.DMLQueryExpr {
	if *expr == nil {
		return nil
	}

	switch e := (*expr).(type) {
	case *ast.ColumnNameExpr:
		return processColumnNameExpr(e)
	case ast.ValueExpr:
		return processValueExpr(e)
	case *ast.FuncCallExpr:
		return processFuncCallExpr(e)
	case *ast.AggregateFuncExpr:
		return processAggregateFuncExpr(e)
	case *ast.BinaryOperationExpr:
		return processBinaryOperationExpr(e)
	case *ast.BetweenExpr:
		return processBetweenExpr(e)
	case *ast.PatternLikeOrIlikeExpr:
		return processPatternLikeExpr(e)
	case *ast.PatternInExpr:
		return processPatternInExpr(e)
	case *ast.ParenthesesExpr:
		return processExprNode(&e.Expr)
	case *ast.UnaryOperationExpr:
		return processUnaryOperationExpr(e)
	case *ast.VariableExpr:
		return processVariableExpr(e)
	case *ast.SubqueryExpr:
		return processSubqueryExpr(e)
	case *ast.ExistsSubqueryExpr:
		return processExistsSubqueryExpr(e)
	case *ast.SetCollationExpr:
		// COLLATE clause: just process the inner expression, ignore collation info
		return processExprNode(&e.Expr)
	case *ast.IsNullExpr:
		return processIsNullExpr(e)
	default:
		fmt.Printf("FIXME: Unsupported expression type: %T\n", *expr)
		return &pb.DMLQueryExpr{
			Operator:  pb.DMLQueryExpr_UNKNOWN,
			ValueType: pb.DMLQueryExpr_UNKNOWN_VALUE,
		}
	}
}

func processColumnNameExpr(e *ast.ColumnNameExpr) *pb.DMLQueryExpr {
	identifier := e.Name.Name.O
	if e.Name.Table.O != "" {
		identifier = e.Name.Table.O + "." + identifier
	}
	return &pb.DMLQueryExpr{
		Operator:   pb.DMLQueryExpr_VALUE,
		ValueType:  pb.DMLQueryExpr_IDENTIFIER,
		Identifier: identifier,
	}
}

func processValueExpr(e ast.ValueExpr) *pb.DMLQueryExpr {
	tp := e.GetType().GetType()

	if types.IsTypeVarchar(tp) {
		return &pb.DMLQueryExpr{
			Operator:  pb.DMLQueryExpr_VALUE,
			ValueType: pb.DMLQueryExpr_STRING,
			String_:   e.GetValue().(string),
		}
	} else if types.IsTypeInteger(tp) {
		return &pb.DMLQueryExpr{
			Operator:  pb.DMLQueryExpr_VALUE,
			ValueType: pb.DMLQueryExpr_INTEGER,
			Integer:   e.GetValue().(int64),
		}
	} else if tp == mysql.TypeNewDecimal {
		decimalValue := ""
		if raw := e.GetValue(); raw != nil {
			if dec, ok := raw.(*types.MyDecimal); ok {
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
	} else if tp == mysql.TypeDouble || types.IsTypeFloat(tp) {
		return &pb.DMLQueryExpr{
			Operator:  pb.DMLQueryExpr_VALUE,
			ValueType: pb.DMLQueryExpr_DOUBLE,
			Double:    e.GetValue().(float64),
		}
	}

	return &pb.DMLQueryExpr{
		Operator:  pb.DMLQueryExpr_VALUE,
		ValueType: pb.DMLQueryExpr_UNKNOWN_VALUE,
		String_:   e.GetString(),
	}
}

func processFuncCallExpr(e *ast.FuncCallExpr) *pb.DMLQueryExpr {
	exprList := make([]*pb.DMLQueryExpr, len(e.Args))
	for i, arg := range e.Args {
		exprList[i] = processExprNode(&arg)
	}
	return &pb.DMLQueryExpr{
		Operator:  pb.DMLQueryExpr_VALUE,
		ValueType: pb.DMLQueryExpr_FUNCTION,
		Function:  e.FnName.O,
		ValueList: exprList,
	}
}

func processAggregateFuncExpr(e *ast.AggregateFuncExpr) *pb.DMLQueryExpr {
	exprList := make([]*pb.DMLQueryExpr, len(e.Args))
	for i, arg := range e.Args {
		exprList[i] = processExprNode(&arg)
	}
	return &pb.DMLQueryExpr{
		Operator:    pb.DMLQueryExpr_VALUE,
		ValueType:   pb.DMLQueryExpr_FUNCTION,
		Function:    e.F,
		ValueList:   exprList,
		IsAggregate: true,
		IsDistinct:  e.Distinct,
	}
}

func processBinaryOperationExpr(e *ast.BinaryOperationExpr) *pb.DMLQueryExpr {
	exprOut := &pb.DMLQueryExpr{}

	switch e.Op {
	case opcode.LT:
		exprOut.Operator = pb.DMLQueryExpr_LT
	case opcode.LE:
		exprOut.Operator = pb.DMLQueryExpr_LTE
	case opcode.GT:
		exprOut.Operator = pb.DMLQueryExpr_GT
	case opcode.GE:
		exprOut.Operator = pb.DMLQueryExpr_GTE
	case opcode.EQ:
		exprOut.Operator = pb.DMLQueryExpr_EQ
	case opcode.NE:
		exprOut.Operator = pb.DMLQueryExpr_NEQ
	case opcode.In:
		exprOut.Operator = pb.DMLQueryExpr_IN
	case opcode.Like:
		exprOut.Operator = pb.DMLQueryExpr_LIKE
	case opcode.IsNull:
		exprOut.Operator = pb.DMLQueryExpr_IS_NULL
	case opcode.LogicAnd:
		exprOut.Operator = pb.DMLQueryExpr_AND
	case opcode.LogicOr:
		exprOut.Operator = pb.DMLQueryExpr_OR
	case opcode.Plus:
		exprOut.Operator = pb.DMLQueryExpr_PLUS
	case opcode.Minus:
		exprOut.Operator = pb.DMLQueryExpr_MINUS
	case opcode.Mul:
		exprOut.Operator = pb.DMLQueryExpr_MUL
	case opcode.Div:
		exprOut.Operator = pb.DMLQueryExpr_DIV
	case opcode.Mod:
		exprOut.Operator = pb.DMLQueryExpr_MOD
	default:
		fmt.Printf("FIXME: Unsupported binary operator: %s\n", e.Op.String())
	}

	if e.Op == opcode.LogicAnd || e.Op == opcode.LogicOr {
		exprOut.Expressions = []*pb.DMLQueryExpr{
			processExprNode(&e.L),
			processExprNode(&e.R),
		}
	} else {
		exprOut.Left = processExprNode(&e.L)
		exprOut.Right = processExprNode(&e.R)
	}

	return exprOut
}

func processBetweenExpr(e *ast.BetweenExpr) *pb.DMLQueryExpr {
	leftA := processExprNode(&e.Expr)
	leftB := processExprNode(&e.Expr)
	low := processExprNode(&e.Left)
	high := processExprNode(&e.Right)

	if e.Not {
		return &pb.DMLQueryExpr{
			Operator: pb.DMLQueryExpr_OR,
			Expressions: []*pb.DMLQueryExpr{
				{Operator: pb.DMLQueryExpr_LT, Left: leftA, Right: low},
				{Operator: pb.DMLQueryExpr_GT, Left: leftB, Right: high},
			},
		}
	}

	return &pb.DMLQueryExpr{
		Operator: pb.DMLQueryExpr_AND,
		Expressions: []*pb.DMLQueryExpr{
			{Operator: pb.DMLQueryExpr_GTE, Left: leftA, Right: low},
			{Operator: pb.DMLQueryExpr_LTE, Left: leftB, Right: high},
		},
	}
}

func processPatternLikeExpr(e *ast.PatternLikeOrIlikeExpr) *pb.DMLQueryExpr {
	op := pb.DMLQueryExpr_LIKE
	if e.Not {
		op = pb.DMLQueryExpr_NOT_LIKE
	}
	return &pb.DMLQueryExpr{
		Operator: op,
		Left:     processExprNode(&e.Expr),
		Right:    processExprNode(&e.Pattern),
	}
}

func processPatternInExpr(e *ast.PatternInExpr) *pb.DMLQueryExpr {
	op := pb.DMLQueryExpr_IN
	if e.Not {
		op = pb.DMLQueryExpr_NOT_IN
	}

	// Handle IN subquery
	if e.Sel != nil {
		subExpr := processSubqueryExpr(e.Sel.(*ast.SubqueryExpr))
		if subExpr != nil {
			subExpr.SubqueryNot = e.Not
		}
		return &pb.DMLQueryExpr{
			Operator: op,
			Left:     processExprNode(&e.Expr),
			Right:    subExpr,
		}
	}

	// Handle IN list (e.g., IN (1, 2, 3))
	valueList := make([]*pb.DMLQueryExpr, 0, len(e.List))
	for i := range e.List {
		item := e.List[i]
		if child := processExprNode(&item); child != nil {
			valueList = append(valueList, child)
		}
	}

	return &pb.DMLQueryExpr{
		Operator: op,
		Left:     processExprNode(&e.Expr),
		Right: &pb.DMLQueryExpr{
			Operator:  pb.DMLQueryExpr_VALUE,
			ValueType: pb.DMLQueryExpr_LIST,
			ValueList: valueList,
		},
	}
}

func processUnaryOperationExpr(e *ast.UnaryOperationExpr) *pb.DMLQueryExpr {
	exprNode := processExprNode(&e.V)

	if exprNode.Operator == pb.DMLQueryExpr_VALUE {
		switch e.Op {
		case opcode.Plus:
			// no-op
		case opcode.Minus:
			switch exprNode.ValueType {
			case pb.DMLQueryExpr_INTEGER:
				exprNode.Integer = -exprNode.Integer
			case pb.DMLQueryExpr_DOUBLE:
				exprNode.Double = -exprNode.Double
			case pb.DMLQueryExpr_DECIMAL:
				if strings.HasPrefix(exprNode.Decimal, "-") {
					exprNode.Decimal = strings.TrimPrefix(exprNode.Decimal, "-")
				} else if exprNode.Decimal != "0" {
					exprNode.Decimal = "-" + exprNode.Decimal
				}
			}
		default:
			fmt.Printf("FIXME: Unsupported unary operator: %s\n", e.Op.String())
		}
	} else {
		fmt.Printf("FIXME: Unsupported unary operator on non-value: %s\n", e.Op.String())
	}

	return exprNode
}

func processVariableExpr(e *ast.VariableExpr) *pb.DMLQueryExpr {
	// User variable like @x is represented as IDENTIFIER with @ prefix
	identifier := "@" + e.Name
	if e.IsGlobal {
		identifier = "@@GLOBAL." + e.Name
	} else if e.IsSystem {
		identifier = "@@" + e.Name
	}

	return &pb.DMLQueryExpr{
		Operator:   pb.DMLQueryExpr_VALUE,
		ValueType:  pb.DMLQueryExpr_IDENTIFIER,
		Identifier: identifier,
	}
}

func processSubqueryExpr(e *ast.SubqueryExpr) *pb.DMLQueryExpr {
	subQuery := &pb.DMLQuery{}

	// e.Query is a ResultSetNode (typically a SelectStmt)
	if selectStmt, ok := e.Query.(*ast.SelectStmt); ok {
		processSelectStmt(subQuery, selectStmt)
	}

	return &pb.DMLQueryExpr{
		Operator:       pb.DMLQueryExpr_VALUE,
		ValueType:      pb.DMLQueryExpr_SUBQUERY,
		Subquery:       subQuery,
		SubqueryExists: e.Exists,
	}
}

func processExistsSubqueryExpr(e *ast.ExistsSubqueryExpr) *pb.DMLQueryExpr {
	// e.Sel is a SubqueryExpr
	subExpr := processSubqueryExpr(e.Sel.(*ast.SubqueryExpr))
	if subExpr != nil {
		subExpr.SubqueryExists = true
		subExpr.SubqueryNot = e.Not
	}
	return subExpr
}

func processIsNullExpr(e *ast.IsNullExpr) *pb.DMLQueryExpr {
	op := pb.DMLQueryExpr_IS_NULL
	if e.Not {
		op = pb.DMLQueryExpr_IS_NOT_NULL
	}
	return &pb.DMLQueryExpr{
		Operator: op,
		Left:     processExprNode(&e.Expr),
	}
}
