// Package parser provides SQL parsing functionality using TiDB parser.
package parser

import (
	"crypto/sha1"
	"encoding/json"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"

	pb "parserlib/pb"
)

// Parser wraps the TiDB parser and provides methods for SQL parsing.
type Parser struct {
	p *parser.Parser
}

// New creates a new Parser instance.
func New() *Parser {
	return &Parser{
		p: parser.New(),
	}
}

// Parse parses the given SQL string and returns a ParseResult protobuf message.
func (p *Parser) Parse(sql string) *pb.ParseResult {
	result := &pb.ParseResult{
		Result: pb.ParseResult_UNKNOWN,
	}

	stmtNodes, warns, err := p.p.Parse(sql, "", "")
	if err != nil {
		result.Result = pb.ParseResult_ERROR
		result.Error = err.Error()
		result.Warnings = make([]string, len(warns))
		for i, warn := range warns {
			result.Warnings[i] = warn.Error()
		}
		return result
	}

	result.Result = pb.ParseResult_SUCCESS
	result.Warnings = make([]string, len(warns))
	for i, warn := range warns {
		result.Warnings[i] = warn.Error()
	}

	for _, stmtNode := range stmtNodes {
		query := processStmtNode(&stmtNode)
		if query != nil {
			result.Statements = append(result.Statements, query)
		}
	}

	return result
}

// Hash computes a SHA1 hash of the normalized SQL statement.
// This is useful for comparing queries that may have different whitespace or casing.
func (p *Parser) Hash(sql string) ([]byte, error) {
	stmtNodes, _, err := p.p.Parse(sql, "", "")
	if err != nil {
		return nil, err
	}

	if len(stmtNodes) == 0 {
		return nil, nil
	}

	normalized := reprNode(&stmtNodes[0])
	hash := sha1.Sum([]byte(normalized))
	return hash[:], nil
}

// Jsonify converts the parsed AST to JSON format for debugging.
func (p *Parser) Jsonify(sql string) ([]byte, error) {
	stmtNodes, _, err := p.p.Parse(sql, "", "")
	if err != nil {
		return nil, err
	}

	if len(stmtNodes) == 0 {
		return nil, nil
	}

	return json.Marshal(stmtNodes[0])
}

// reprNode reconstructs the SQL string from an AST node.
func reprNode(node *ast.StmtNode) string {
	var sb strings.Builder
	err := (*node).Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	if err != nil {
		return ""
	}
	return sb.String()
}
