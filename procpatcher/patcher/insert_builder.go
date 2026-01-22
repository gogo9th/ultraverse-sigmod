package patcher

import (
	"fmt"
	"strings"
)

// BuildHintInsertSQL builds the SQL for inserting procedure hint
// procName: name of the procedure
// params: procedure parameters
// vars: local variables visible at this point
func BuildHintInsertSQL(procName string, params []Variable, vars []Variable) string {
	// Build JSON_OBJECT for args (parameters)
	argsJSON := buildJSONObject(params)

	// Build JSON_OBJECT for vars (local variables)
	varsJSON := buildJSONObject(vars)

	// Build the INSERT statement
	return fmt.Sprintf(
		"INSERT INTO __ULTRAVERSE_PROCEDURE_HINT (callid, procname, args, vars) VALUES (UUID_SHORT(), '%s', %s, %s)",
		escapeSQLString(procName),
		argsJSON,
		varsJSON,
	)
}

// buildJSONObject creates a JSON_OBJECT() call from variables
func buildJSONObject(vars []Variable) string {
	if len(vars) == 0 {
		return "JSON_OBJECT()"
	}

	var parts []string
	for _, v := range vars {
		// JSON_OBJECT('name', @name, 'name2', @name2, ...)
		// For procedure variables, we use the variable name directly (no @)
		parts = append(parts, fmt.Sprintf("'%s', %s", escapeSQLString(v.Name), v.Name))
	}

	return fmt.Sprintf("JSON_OBJECT(%s)", strings.Join(parts, ", "))
}

// escapeSQLString escapes single quotes in SQL strings
func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
