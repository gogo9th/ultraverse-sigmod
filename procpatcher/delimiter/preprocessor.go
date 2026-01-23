package delimiter

import (
	"strings"
)

// Statement represents a SQL statement separated by delimiter directives.
// Start/End are byte offsets in the original SQL (End is exclusive, delimiter excluded).
type Statement struct {
	Text  string
	Start int
	End   int
	// HasCode is true if the statement contains non-whitespace, non-comment tokens.
	HasCode bool
}

// NormalizeDelimiters removes DELIMITER directives and normalizes statement
// terminators to semicolons so the SQL can be parsed by tidb/parser.
func NormalizeDelimiters(sql string) (string, error) {
	statements, err := SplitStatements(sql)
	if err != nil {
		return "", err
	}

	var out strings.Builder
	first := true
	for _, stmt := range statements {
		if !stmt.HasCode {
			continue
		}
		if !first {
			out.WriteString("\n")
		}
		first = false
		out.WriteString(strings.TrimSpace(stmt.Text))
		out.WriteString(";")
	}

	return out.String(), nil
}

// SplitStatements splits SQL by honoring DELIMITER directives and tracking
// string/comment state so delimiters inside them are ignored.
func SplitStatements(sql string) ([]Statement, error) {
	currentDelimiter := ";"
	data := []byte(sql)

	var statements []Statement
	stmtStart := 0
	hasContent := false

	lineStart := true
	inSingle := false
	inDouble := false
	inBacktick := false
	inLineComment := false
	inBlockComment := false

	flush := func(end int) {
		if end < stmtStart {
			stmtStart = end
		}
		if end > stmtStart {
			text := sql[stmtStart:end]
			statements = append(statements, Statement{
				Text:    text,
				Start:   stmtStart,
				End:     end,
				HasCode: hasContent,
			})
		}
		hasContent = false
	}

	for i := 0; i < len(data); {
		if lineStart && !inSingle && !inDouble && !inBacktick && !inLineComment && !inBlockComment {
			if next, delim, ok := consumeDelimiterDirectiveBytes(data, i); ok {
				flush(i)
				stmtStart = next
				if delim != "" {
					currentDelimiter = delim
				}
				i = next
				lineStart = true
				continue
			}
		}

		ch := data[i]

		if inLineComment {
			if isLineBreakByte(ch) {
				inLineComment = false
				lineStart = true
			} else {
				lineStart = false
			}
			i++
			continue
		}

		if inBlockComment {
			if ch == '*' && i+1 < len(data) && data[i+1] == '/' {
				i += 2
				inBlockComment = false
				lineStart = false
				continue
			}
			if isLineBreakByte(ch) {
				lineStart = true
			} else {
				lineStart = false
			}
			i++
			continue
		}

		if inSingle {
			if ch == '\\' && i+1 < len(data) {
				i += 2
				lineStart = false
				continue
			}
			if ch == '\'' {
				if i+1 < len(data) && data[i+1] == '\'' {
					i += 2
					lineStart = false
					continue
				}
				inSingle = false
			}
			if isLineBreakByte(ch) {
				lineStart = true
			} else {
				lineStart = false
			}
			i++
			continue
		}

		if inDouble {
			if ch == '\\' && i+1 < len(data) {
				i += 2
				lineStart = false
				continue
			}
			if ch == '"' {
				if i+1 < len(data) && data[i+1] == '"' {
					i += 2
					lineStart = false
					continue
				}
				inDouble = false
			}
			if isLineBreakByte(ch) {
				lineStart = true
			} else {
				lineStart = false
			}
			i++
			continue
		}

		if inBacktick {
			if ch == '`' {
				if i+1 < len(data) && data[i+1] == '`' {
					i += 2
					lineStart = false
					continue
				}
				inBacktick = false
			}
			if isLineBreakByte(ch) {
				lineStart = true
			} else {
				lineStart = false
			}
			i++
			continue
		}

		if matchesDelimiterBytes(data, i, currentDelimiter) {
			flush(i)
			i += len(currentDelimiter)
			stmtStart = i
			lineStart = false
			continue
		}

		if ch == '-' && i+1 < len(data) && data[i+1] == '-' {
			if i+2 >= len(data) || isSpaceOrControlByte(data[i+2]) {
				inLineComment = true
				i += 2
				lineStart = false
				continue
			}
		}

		if ch == '#' {
			inLineComment = true
			i++
			lineStart = false
			continue
		}

		if ch == '/' && i+1 < len(data) && data[i+1] == '*' {
			inBlockComment = true
			i += 2
			lineStart = false
			continue
		}

		if ch == '\'' {
			inSingle = true
		} else if ch == '"' {
			inDouble = true
		} else if ch == '`' {
			inBacktick = true
		}

		if !isWhitespaceByte(ch) {
			hasContent = true
		}

		if isLineBreakByte(ch) {
			lineStart = true
		} else {
			lineStart = false
		}
		i++
	}

	flush(len(data))
	return statements, nil
}

// PickDelimiter chooses a delimiter that does not appear in the SQL text.
func PickDelimiter(sql string) string {
	candidates := []string{"$$", "//", ";;", "||", "##", "@@"}
	for _, cand := range candidates {
		if !strings.Contains(sql, cand) {
			return cand
		}
	}

	base := "__ULTRAVERSE_DELIM__"
	if !strings.Contains(sql, base) {
		return base
	}
	for i := 1; ; i++ {
		cand := base + "_" + itoa(i)
		if !strings.Contains(sql, cand) {
			return cand
		}
	}
}

func consumeDelimiterDirectiveBytes(data []byte, start int) (next int, delim string, ok bool) {
	i := start
	for i < len(data) && isHorizontalSpaceByte(data[i]) {
		i++
	}
	if !matchKeywordAtBytes(data, i, "DELIMITER") {
		return start, "", false
	}

	j := i + len("DELIMITER")
	if j >= len(data) || !isWhitespaceByte(data[j]) {
		return start, "", false
	}
	for j < len(data) && isHorizontalSpaceByte(data[j]) {
		j++
	}
	if j >= len(data) || isLineBreakByte(data[j]) {
		return start, "", false
	}

	k := j
	for k < len(data) && !isWhitespaceByte(data[k]) {
		k++
	}
	delim = string(data[j:k])

	for k < len(data) && !isLineBreakByte(data[k]) {
		k++
	}
	if k < len(data) && data[k] == '\r' {
		k++
		if k < len(data) && data[k] == '\n' {
			k++
		}
	} else if k < len(data) && data[k] == '\n' {
		k++
	}

	return k, delim, true
}

func matchesDelimiterBytes(data []byte, pos int, delim string) bool {
	if delim == "" {
		return false
	}
	if pos+len(delim) > len(data) {
		return false
	}
	for i := 0; i < len(delim); i++ {
		if data[pos+i] != delim[i] {
			return false
		}
	}
	return true
}

func matchKeywordAtBytes(data []byte, pos int, keyword string) bool {
	if pos+len(keyword) > len(data) {
		return false
	}
	for i := 0; i < len(keyword); i++ {
		if !equalIgnoreCaseByte(data[pos+i], keyword[i]) {
			return false
		}
	}
	after := pos + len(keyword)
	if after < len(data) && (isAlphaNumericByte(data[after]) || data[after] == '_') {
		return false
	}
	return true
}

func equalIgnoreCaseByte(a byte, b byte) bool {
	if a == b {
		return true
	}
	if a >= 'a' && a <= 'z' {
		a -= 32
	}
	if b >= 'a' && b <= 'z' {
		b -= 32
	}
	return a == b
}

func isAlphaNumericByte(r byte) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}

func isWhitespaceByte(r byte) bool {
	return r == ' ' || r == '\t' || r == '\n' || r == '\r'
}

func isHorizontalSpaceByte(r byte) bool {
	return r == ' ' || r == '\t'
}

func isLineBreakByte(r byte) bool {
	return r == '\n' || r == '\r'
}

func isSpaceOrControlByte(r byte) bool {
	return r == ' ' || r == '\t' || r == '\n' || r == '\r' || r == '\f' || r == '\v'
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
