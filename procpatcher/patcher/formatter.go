package patcher

import (
	"strings"
)

// FormatProcedureSQL formats procedure SQL with proper indentation and line breaks
func FormatProcedureSQL(sql string) string {
	var result strings.Builder
	indent := 0
	inSingleQuote := false
	inDoubleQuote := false
	inBacktick := false

	runes := []rune(sql)
	i := 0

	for i < len(runes) {
		ch := runes[i]

		// Handle escape sequences in strings
		if (inSingleQuote || inDoubleQuote) && ch == '\\' && i+1 < len(runes) {
			result.WriteRune(ch)
			i++
			result.WriteRune(runes[i])
			i++
			continue
		}

		// Track string state
		if ch == '\'' && !inDoubleQuote && !inBacktick {
			inSingleQuote = !inSingleQuote
			result.WriteRune(ch)
			i++
			continue
		}
		if ch == '"' && !inSingleQuote && !inBacktick {
			inDoubleQuote = !inDoubleQuote
			result.WriteRune(ch)
			i++
			continue
		}
		if ch == '`' && !inSingleQuote && !inDoubleQuote {
			inBacktick = !inBacktick
			result.WriteRune(ch)
			i++
			continue
		}

		// Skip if inside string
		if inSingleQuote || inDoubleQuote || inBacktick {
			result.WriteRune(ch)
			i++
			continue
		}

		// Check for BEGIN keyword
		if matchKeywordAt(runes, i, "BEGIN") {
			result.WriteString("BEGIN")
			indent++
			result.WriteString("\n")
			result.WriteString(strings.Repeat("    ", indent))
			i += 5
			skipWhitespace(runes, &i)
			continue
		}

		// Check for END keyword (but not END IF, END WHILE, etc.)
		if matchKeywordAt(runes, i, "END") {
			afterEnd := i + 3
			skipWhitespaceAt(runes, &afterEnd)

			// Check what follows END
			if matchKeywordAt(runes, afterEnd, "IF") ||
				matchKeywordAt(runes, afterEnd, "WHILE") ||
				matchKeywordAt(runes, afterEnd, "LOOP") ||
				matchKeywordAt(runes, afterEnd, "REPEAT") ||
				matchKeywordAt(runes, afterEnd, "CASE") ||
				matchKeywordAt(runes, afterEnd, "FOR") {
				// END IF, END WHILE, etc. - just write and add newline
				result.WriteString("END")
				i += 3
			} else {
				// Block END - decrease indent first
				if indent > 0 {
					indent--
				}
				result.WriteString("\n")
				result.WriteString(strings.Repeat("    ", indent))
				result.WriteString("END")
				i += 3
			}
			continue
		}

		// Check for THEN keyword
		if matchKeywordAt(runes, i, "THEN") {
			result.WriteString("THEN")
			result.WriteString("\n")
			indent++
			result.WriteString(strings.Repeat("    ", indent))
			i += 4
			skipWhitespace(runes, &i)
			continue
		}

		// Check for ELSE keyword (but not ELSEIF)
		if matchKeywordAt(runes, i, "ELSE") && !matchKeywordAt(runes, i, "ELSEIF") {
			if indent > 0 {
				indent--
			}
			result.WriteString("\n")
			result.WriteString(strings.Repeat("    ", indent))
			result.WriteString("ELSE")
			indent++
			result.WriteString("\n")
			result.WriteString(strings.Repeat("    ", indent))
			i += 4
			skipWhitespace(runes, &i)
			continue
		}

		// Check for ELSEIF keyword
		if matchKeywordAt(runes, i, "ELSEIF") {
			if indent > 0 {
				indent--
			}
			result.WriteString("\n")
			result.WriteString(strings.Repeat("    ", indent))
			result.WriteString("ELSEIF")
			i += 6
			continue
		}

		// Check for DO keyword (WHILE ... DO)
		if matchKeywordAt(runes, i, "DO") {
			result.WriteString("DO")
			result.WriteString("\n")
			indent++
			result.WriteString(strings.Repeat("    ", indent))
			i += 2
			skipWhitespace(runes, &i)
			continue
		}

		// Handle semicolon - add newline after
		if ch == ';' {
			result.WriteRune(ch)
			// Check if next non-whitespace is END, ELSE, ELSEIF, or another control keyword
			nextIdx := i + 1
			skipWhitespaceAt(runes, &nextIdx)

			if nextIdx < len(runes) {
				// Check if we should decrease indent before next statement
				if matchKeywordAt(runes, nextIdx, "END") {
					afterEnd := nextIdx + 3
					skipWhitespaceAt(runes, &afterEnd)
					if matchKeywordAt(runes, afterEnd, "IF") ||
						matchKeywordAt(runes, afterEnd, "WHILE") ||
						matchKeywordAt(runes, afterEnd, "LOOP") ||
						matchKeywordAt(runes, afterEnd, "REPEAT") ||
						matchKeywordAt(runes, afterEnd, "CASE") ||
						matchKeywordAt(runes, afterEnd, "FOR") {
						// END IF, etc. - decrease indent
						if indent > 0 {
							indent--
						}
					}
				} else if matchKeywordAt(runes, nextIdx, "ELSE") || matchKeywordAt(runes, nextIdx, "ELSEIF") {
					// Will be handled by ELSE/ELSEIF processing
				}

				result.WriteString("\n")
				result.WriteString(strings.Repeat("    ", indent))
			}
			i++
			skipWhitespace(runes, &i)
			continue
		}

		result.WriteRune(ch)
		i++
	}

	return result.String()
}

// matchKeywordAt checks if keyword appears at position i (case insensitive) with word boundary
func matchKeywordAt(runes []rune, i int, keyword string) bool {
	if i+len(keyword) > len(runes) {
		return false
	}

	// Check word boundary before
	if i > 0 {
		prev := runes[i-1]
		if isAlphaNumeric(prev) || prev == '_' {
			return false
		}
	}

	// Check keyword match (case insensitive)
	for j := 0; j < len(keyword); j++ {
		r := runes[i+j]
		k := rune(keyword[j])
		if !equalIgnoreCase(r, k) {
			return false
		}
	}

	// Check word boundary after
	afterPos := i + len(keyword)
	if afterPos < len(runes) {
		after := runes[afterPos]
		if isAlphaNumeric(after) || after == '_' {
			return false
		}
	}

	return true
}

func equalIgnoreCase(a, b rune) bool {
	if a == b {
		return true
	}
	// Convert to uppercase for comparison
	if a >= 'a' && a <= 'z' {
		a -= 32
	}
	if b >= 'a' && b <= 'z' {
		b -= 32
	}
	return a == b
}

func isAlphaNumeric(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}

func skipWhitespace(runes []rune, i *int) {
	for *i < len(runes) && (runes[*i] == ' ' || runes[*i] == '\t' || runes[*i] == '\n' || runes[*i] == '\r') {
		(*i)++
	}
}

func skipWhitespaceAt(runes []rune, i *int) {
	for *i < len(runes) && (runes[*i] == ' ' || runes[*i] == '\t' || runes[*i] == '\n' || runes[*i] == '\r') {
		(*i)++
	}
}
