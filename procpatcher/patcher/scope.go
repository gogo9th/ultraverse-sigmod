package patcher

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// ParamDirection represents the direction of a procedure parameter
type ParamDirection int

const (
	ParamIn ParamDirection = iota
	ParamOut
	ParamInOut
)

// Variable represents a variable (parameter or local variable)
type Variable struct {
	Name      string
	Direction ParamDirection // Only meaningful for parameters
	DataType  string         // SQL data type
}

// Scope tracks variables in a particular scope
type Scope struct {
	parent    *Scope
	variables map[string]Variable
	label     string // Block label (for labeled BEGIN...END)
}

// NewScope creates a new scope
func NewScope(parent *Scope, label string) *Scope {
	return &Scope{
		parent:    parent,
		variables: make(map[string]Variable),
		label:     label,
	}
}

// AddVariable adds a variable to this scope
func (s *Scope) AddVariable(v Variable) {
	s.variables[v.Name] = v
}

// GetAllVisibleVariables returns all variables visible from this scope
func (s *Scope) GetAllVisibleVariables() []Variable {
	seen := make(map[string]bool)
	var result []Variable

	for scope := s; scope != nil; scope = scope.parent {
		for name, v := range scope.variables {
			if !seen[name] {
				seen[name] = true
				result = append(result, v)
			}
		}
	}

	return result
}

// GetVariablesInCurrentScope returns only variables declared in the current scope
func (s *Scope) GetVariablesInCurrentScope() []Variable {
	var result []Variable
	for _, v := range s.variables {
		result = append(result, v)
	}
	return result
}

// ExtractParameters extracts parameters from a ProcedureInfo
func ExtractParameters(info *ast.ProcedureInfo) []Variable {
	var params []Variable

	if info == nil || info.ProcedureParam == nil {
		return params
	}

	for _, param := range info.ProcedureParam {
		dir := ParamIn
		switch param.Paramstatus {
		case ast.MODE_IN:
			dir = ParamIn
		case ast.MODE_OUT:
			dir = ParamOut
		case ast.MODE_INOUT:
			dir = ParamInOut
		}

		dataType := ""
		if param.ParamType != nil {
			dataType = param.ParamType.String()
		}

		params = append(params, Variable{
			Name:      param.ParamName,
			Direction: dir,
			DataType:  dataType,
		})
	}

	return params
}

// ExtractDeclaredVariables extracts DECLARE variables from a ProcedureBlock
func ExtractDeclaredVariables(block *ast.ProcedureBlock) []Variable {
	var vars []Variable

	if block == nil {
		return vars
	}

	for _, decl := range block.ProcedureVars {
		if varDecl, ok := decl.(*ast.ProcedureDecl); ok {
			dataType := ""
			if varDecl.DeclType != nil {
				dataType = (*varDecl.DeclType).String()
			}

			for _, name := range varDecl.DeclNames {
				vars = append(vars, Variable{
					Name:     name,
					DataType: dataType,
				})
			}
		}
	}

	return vars
}

// ScopeTracker helps track variable scopes while traversing AST
type ScopeTracker struct {
	rootScope    *Scope
	currentScope *Scope
	parameters   []Variable
}

// NewScopeTracker creates a new scope tracker with procedure parameters
func NewScopeTracker(params []Variable) *ScopeTracker {
	root := NewScope(nil, "")
	for _, p := range params {
		root.AddVariable(p)
	}

	return &ScopeTracker{
		rootScope:    root,
		currentScope: root,
		parameters:   params,
	}
}

// EnterBlock enters a new scope for a BEGIN...END block
func (st *ScopeTracker) EnterBlock(label string, declaredVars []Variable) {
	newScope := NewScope(st.currentScope, label)
	for _, v := range declaredVars {
		newScope.AddVariable(v)
	}
	st.currentScope = newScope
}

// LeaveBlock leaves the current scope
func (st *ScopeTracker) LeaveBlock() {
	if st.currentScope.parent != nil {
		st.currentScope = st.currentScope.parent
	}
}

// GetCurrentScope returns the current scope
func (st *ScopeTracker) GetCurrentScope() *Scope {
	return st.currentScope
}

// GetParameters returns the procedure parameters
func (st *ScopeTracker) GetParameters() []Variable {
	return st.parameters
}
