package patcher

import (
	"sort"
	"strings"
)

type textInsertion struct {
	Offset int
	Text   string
	Seq    int
}

type textDeletion struct {
	Offset int
	Length int
	Seq    int
}

func applyInsertions(sql string, insertions []textInsertion) string {
	if len(insertions) == 0 {
		return sql
	}

	sort.Slice(insertions, func(i, j int) bool {
		if insertions[i].Offset != insertions[j].Offset {
			return insertions[i].Offset > insertions[j].Offset
		}
		return insertions[i].Seq > insertions[j].Seq
	})

	buf := []byte(sql)
	for _, ins := range insertions {
		if ins.Offset < 0 || ins.Offset > len(buf) {
			continue
		}
		prefix := append([]byte{}, buf[:ins.Offset]...)
		suffix := buf[ins.Offset:]
		buf = append(prefix, append([]byte(ins.Text), suffix...)...)
	}

	return string(buf)
}

func applyDeletions(sql string, deletions []textDeletion) string {
	if len(deletions) == 0 {
		return sql
	}

	sort.Slice(deletions, func(i, j int) bool {
		if deletions[i].Offset != deletions[j].Offset {
			return deletions[i].Offset > deletions[j].Offset
		}
		return deletions[i].Seq > deletions[j].Seq
	})

	buf := []byte(sql)
	for _, del := range deletions {
		if del.Offset < 0 || del.Length <= 0 || del.Offset > len(buf) {
			continue
		}
		end := del.Offset + del.Length
		if end > len(buf) {
			end = len(buf)
		}
		prefix := append([]byte{}, buf[:del.Offset]...)
		suffix := buf[end:]
		buf = append(prefix, suffix...)
	}

	return string(buf)
}

func visibleLocals(scope *Scope, params []Variable) []Variable {
	if scope == nil {
		return nil
	}
	paramSet := make(map[string]struct{}, len(params))
	for _, p := range params {
		paramSet[p.Name] = struct{}{}
	}

	var locals []Variable
	for _, v := range scope.GetAllVisibleVariables() {
		if _, ok := paramSet[v.Name]; !ok {
			locals = append(locals, v)
		}
	}
	return locals
}

func detectNewline(text string) string {
	if strings.Contains(text, "\r\n") {
		return "\r\n"
	}
	return "\n"
}

func findLineStart(text string, pos int) int {
	if pos < 0 {
		return 0
	}
	if pos > len(text) {
		pos = len(text)
	}
	for pos > 0 {
		ch := text[pos-1]
		if ch == '\n' || ch == '\r' {
			break
		}
		pos--
	}
	return pos
}

func lineIndent(text string, lineStart int) string {
	if lineStart < 0 {
		lineStart = 0
	}
	if lineStart > len(text) {
		lineStart = len(text)
	}
	i := lineStart
	for i < len(text) {
		if text[i] != ' ' && text[i] != '\t' {
			break
		}
		i++
	}
	return text[lineStart:i]
}

func findProcedureEndInsertion(text string) (lineStart int, indent string, ok bool) {
	pos, ok := findLastKeywordPosition(text, "END")
	if !ok {
		return 0, "", false
	}
	lineStart = findLineStart(text, pos)
	indent = indentOfPreviousNonEmptyLine(text, lineStart)
	if indent == "" {
		endIndent := lineIndent(text, lineStart)
		if endIndent == "" {
			indent = "    "
		} else {
			indent = endIndent + "    "
		}
	}
	return lineStart, indent, true
}

func indentOfPreviousNonEmptyLine(text string, lineStart int) string {
	if lineStart <= 0 {
		return ""
	}

	end := lineStart - 1
	for end >= 0 {
		// Skip line breaks.
		for end >= 0 && (text[end] == '\n' || text[end] == '\r') {
			end--
		}
		if end < 0 {
			return ""
		}

		start := end
		for start > 0 && text[start-1] != '\n' && text[start-1] != '\r' {
			start--
		}
		line := text[start : end+1]
		if strings.TrimSpace(line) != "" {
			return lineIndent(text, start)
		}
		end = start - 1
	}
	return ""
}

func findLastKeywordPosition(text string, keyword string) (int, bool) {
	data := []byte(text)
	upper := strings.ToUpper(keyword)
	last := -1

	inSingle := false
	inDouble := false
	inBacktick := false
	inLineComment := false
	inBlockComment := false

	for i := 0; i < len(data); i++ {
		ch := data[i]

		if inLineComment {
			if isLineBreakByte(ch) {
				inLineComment = false
			}
			continue
		}

		if inBlockComment {
			if ch == '*' && i+1 < len(data) && data[i+1] == '/' {
				i++
				inBlockComment = false
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
			continue
		}

		if ch == '-' && i+1 < len(data) && data[i+1] == '-' {
			if i+2 >= len(data) || isSpaceOrControlByte(data[i+2]) {
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

		if matchKeywordAtBytes(data, i, upper) {
			last = i
		}
	}

	if last == -1 {
		return 0, false
	}
	return last, true
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
	before := pos - 1
	if before >= 0 && (isAlphaNumericByte(data[before]) || data[before] == '_') {
		return false
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

func isLineBreakByte(r byte) bool {
	return r == '\n' || r == '\r'
}

func isSpaceOrControlByte(r byte) bool {
	return r == ' ' || r == '\t' || r == '\n' || r == '\r' || r == '\f' || r == '\v'
}
