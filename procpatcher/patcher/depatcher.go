package patcher

import (
	"fmt"
	"strings"

	"procpatcher/delimiter"
)

// Depatch removes Ultraverse procedure hint statements from SQL.
func Depatch(sql string) (*PatchResult, error) {
	result := &PatchResult{
		Warnings: []string{},
	}

	statements, err := delimiter.SplitStatements(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to split statements: %w", err)
	}

	var deletions []textDeletion
	seq := 0
	for _, stmt := range statements {
		if !stmt.HasCode {
			continue
		}
		if !containsHintTargets(stmt.Text) {
			continue
		}

		for _, del := range findHintDeletions(stmt.Text) {
			deletions = append(deletions, textDeletion{
				Offset: stmt.Start + del.Offset,
				Length: del.Length,
				Seq:    seq,
			})
			seq++
		}
	}

	result.PatchedSQL = applyDeletions(sql, deletions)
	return result, nil
}

func containsHintTargets(text string) bool {
	lower := strings.ToLower(text)
	return strings.Contains(lower, "__ultraverse_procedure_hint") ||
		strings.Contains(lower, "__ultraverse_callinfo")
}

func hasHintTableReference(text string) bool {
	tokens := tokenizeHintSegment(text)
	for _, tok := range tokens {
		if tok.text == "__ultraverse_procedure_hint" {
			return true
		}
	}
	return false
}

func findHintDeletions(stmtText string) []textDeletion {
	if stmtText == "" {
		return nil
	}

	bodyStart := findRoutineBodyStart(stmtText)
	if bodyStart < 0 || bodyStart > len(stmtText) {
		bodyStart = 0
	}

	data := []byte(stmtText)
	segStart := bodyStart
	var deletions []textDeletion

	inSingle := false
	inDouble := false
	inBacktick := false
	inLineComment := false
	inBlockComment := false
	lineStart := true

	for i := bodyStart; i < len(data); {
		ch := data[i]

		if inLineComment {
			if isLineBreakByte(ch) {
				inLineComment = false
				lineStart = true
			}
			i++
			continue
		}

		if inBlockComment {
			if ch == '*' && i+1 < len(data) && data[i+1] == '/' {
				i += 2
				inBlockComment = false
				continue
			}
			if isLineBreakByte(ch) {
				lineStart = true
			}
			i++
			continue
		}

		if inSingle {
			if ch == '\\' && i+1 < len(data) {
				i += 2
				continue
			}
			if ch == '\'' {
				if i+1 < len(data) && data[i+1] == '\'' {
					i += 2
					continue
				}
				inSingle = false
			}
			if isLineBreakByte(ch) {
				lineStart = true
			}
			i++
			continue
		}

		if inDouble {
			if ch == '\\' && i+1 < len(data) {
				i += 2
				continue
			}
			if ch == '"' {
				if i+1 < len(data) && data[i+1] == '"' {
					i += 2
					continue
				}
				inDouble = false
			}
			if isLineBreakByte(ch) {
				lineStart = true
			}
			i++
			continue
		}

		if inBacktick {
			if ch == '`' {
				if i+1 < len(data) && data[i+1] == '`' {
					i += 2
					continue
				}
				inBacktick = false
			}
			if isLineBreakByte(ch) {
				lineStart = true
			}
			i++
			continue
		}

		if ch == '-' && i+1 < len(data) && data[i+1] == '-' {
			if lineStart || i+2 >= len(data) || isSpaceOrControlByte(data[i+2]) {
				inLineComment = true
				i += 2
				continue
			}
		}
		if ch == '#' {
			inLineComment = true
			i++
			continue
		}
		if ch == '/' && i+1 < len(data) && data[i+1] == '*' {
			inBlockComment = true
			i += 2
			continue
		}
		if ch == '\'' {
			inSingle = true
			i++
			continue
		}
		if ch == '"' {
			inDouble = true
			i++
			continue
		}
		if ch == '`' {
			inBacktick = true
			i++
			continue
		}

		if ch == ';' {
			segEnd := i + 1
			if segStart < segEnd {
				if start, ok := hintRemovalStart(stmtText[segStart:segEnd]); ok {
					start = segStart + start
					deletions = append(deletions, textDeletion{
						Offset: start,
						Length: segEnd - start,
						Seq:    len(deletions),
					})
				}
			}
			segStart = segEnd
		}
		if isLineBreakByte(ch) {
			lineStart = true
		} else if ch != ' ' && ch != '\t' {
			lineStart = false
		}
		i++
	}

	return deletions
}

func findRoutineBodyStart(text string) int {
	data := []byte(text)

	inSingle := false
	inDouble := false
	inBacktick := false
	inLineComment := false
	inBlockComment := false
	lineStart := true

	for i := 0; i < len(data); i++ {
		ch := data[i]

		if inLineComment {
			if isLineBreakByte(ch) {
				inLineComment = false
				lineStart = true
			}
			continue
		}
		if inBlockComment {
			if ch == '*' && i+1 < len(data) && data[i+1] == '/' {
				i++
				inBlockComment = false
			}
			if isLineBreakByte(ch) {
				lineStart = true
			}
			continue
		}
		if inSingle {
			if ch == '\\' && i+1 < len(data) {
				i++
				continue
			}
			if ch == '\'' {
				if i+1 < len(data) && data[i+1] == '\'' {
					i++
					continue
				}
				inSingle = false
			}
			if isLineBreakByte(ch) {
				lineStart = true
			}
			continue
		}
		if inDouble {
			if ch == '\\' && i+1 < len(data) {
				i++
				continue
			}
			if ch == '"' {
				if i+1 < len(data) && data[i+1] == '"' {
					i++
					continue
				}
				inDouble = false
			}
			if isLineBreakByte(ch) {
				lineStart = true
			}
			continue
		}
		if inBacktick {
			if ch == '`' {
				if i+1 < len(data) && data[i+1] == '`' {
					i++
					continue
				}
				inBacktick = false
			}
			if isLineBreakByte(ch) {
				lineStart = true
			}
			continue
		}

		if ch == '-' && i+1 < len(data) && data[i+1] == '-' {
			if lineStart || i+2 >= len(data) || isSpaceOrControlByte(data[i+2]) {
				inLineComment = true
				i++
				continue
			}
		}
		if ch == '#' {
			inLineComment = true
			continue
		}
		if ch == '/' && i+1 < len(data) && data[i+1] == '*' {
			inBlockComment = true
			i++
			continue
		}
		if ch == '\'' {
			inSingle = true
			continue
		}
		if ch == '"' {
			inDouble = true
			continue
		}
		if ch == '`' {
			inBacktick = true
			continue
		}

		if matchKeywordAtBytes(data, i, "BEGIN") {
			return i + len("BEGIN")
		}
		if isLineBreakByte(ch) {
			lineStart = true
		} else if ch != ' ' && ch != '\t' {
			lineStart = false
		}
	}

	return 0
}

type hintToken struct {
	text string
	pos  int
}

func hintRemovalStart(segment string) (int, bool) {
	tokens := tokenizeHintSegment(segment)
	if len(tokens) == 0 {
		return 0, false
	}

	for i := 0; i+1 < len(tokens); i++ {
		if tokens[i].text == "declare" && tokens[i+1].text == "__ultraverse_callinfo" {
			return statementStart(segment, tokens[i].pos), true
		}
	}

	for i := 0; i < len(tokens); i++ {
		if tokens[i].text != "insert" {
			continue
		}
		idx := i + 1
		for idx < len(tokens) && isInsertModifier(tokens[idx].text) {
			idx++
		}
		if idx < len(tokens) && tokens[idx].text == "into" {
			idx++
		}
		if idx >= len(tokens) {
			continue
		}
		if tokens[idx].text == "__ultraverse_procedure_hint" {
			return statementStart(segment, tokens[i].pos), true
		}
		if idx+2 < len(tokens) && tokens[idx+1].text == "." && tokens[idx+2].text == "__ultraverse_procedure_hint" {
			return statementStart(segment, tokens[i].pos), true
		}
	}

	return 0, false
}

func statementStart(segment string, pos int) int {
	start := pos
	for start > 0 {
		prev := segment[start-1]
		if prev == ' ' || prev == '\t' {
			start--
			continue
		}
		if prev == '\n' || prev == '\r' {
			break
		}
		break
	}
	return start
}

func tokenizeHintSegment(segment string) []hintToken {
	data := []byte(segment)
	var tokens []hintToken
	lineStart := true

	for i := 0; i < len(data); {
		ch := data[i]
		if isLineBreakByte(ch) {
			lineStart = true
			i++
			continue
		}
		if ch == ' ' || ch == '\t' {
			i++
			continue
		}

		if ch == '-' && i+1 < len(data) && data[i+1] == '-' {
			if lineStart || i+2 >= len(data) || isSpaceOrControlByte(data[i+2]) {
				i += 2
				for i < len(data) && !isLineBreakByte(data[i]) {
					i++
				}
				lineStart = true
				continue
			}
		}
		if ch == '#' {
			i++
			for i < len(data) && !isLineBreakByte(data[i]) {
				i++
			}
			lineStart = true
			continue
		}
		if ch == '/' && i+1 < len(data) && data[i+1] == '*' {
			i += 2
			for i < len(data) {
				if data[i] == '*' && i+1 < len(data) && data[i+1] == '/' {
					i += 2
					break
				}
				i++
			}
			continue
		}

		if ch == '\'' {
			i++
			for i < len(data) {
				if data[i] == '\\' && i+1 < len(data) {
					i += 2
					continue
				}
				if data[i] == '\'' {
					if i+1 < len(data) && data[i+1] == '\'' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			continue
		}
		if ch == '"' {
			i++
			for i < len(data) {
				if data[i] == '\\' && i+1 < len(data) {
					i += 2
					continue
				}
				if data[i] == '"' {
					if i+1 < len(data) && data[i+1] == '"' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			continue
		}

		if ch == '`' {
			start := i
			i++
			for i < len(data) {
				if data[i] == '`' {
					if i+1 < len(data) && data[i+1] == '`' {
						i += 2
						continue
					}
					break
				}
				i++
			}
			name := strings.ToLower(string(data[start+1 : i]))
			tokens = append(tokens, hintToken{text: name, pos: start})
			if i < len(data) && data[i] == '`' {
				i++
			}
			lineStart = false
			continue
		}

		if isIdentStartByte(ch) {
			start := i
			i++
			for i < len(data) && isIdentPartByte(data[i]) {
				i++
			}
			tokens = append(tokens, hintToken{
				text: strings.ToLower(string(data[start:i])),
				pos:  start,
			})
			lineStart = false
			continue
		}

		if ch == '.' {
			tokens = append(tokens, hintToken{text: ".", pos: i})
			i++
			lineStart = false
			continue
		}

		lineStart = false
		i++
	}

	return tokens
}

func isIdentStartByte(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isIdentPartByte(ch byte) bool {
	return isIdentStartByte(ch) || (ch >= '0' && ch <= '9')
}

func isInsertModifier(token string) bool {
	switch token {
	case "low_priority", "high_priority", "delayed", "ignore":
		return true
	default:
		return false
	}
}
