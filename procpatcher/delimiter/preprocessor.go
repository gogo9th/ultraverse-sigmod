package delimiter

import (
	"regexp"
	"strings"
)

// DelimiterInfo stores information about a DELIMITER statement
type DelimiterInfo struct {
	OriginalLine string // The original DELIMITER line
	Delimiter    string // The delimiter value (e.g., "$$", "//", ";")
}

var delimiterRegex = regexp.MustCompile(`(?im)^[ \t]*DELIMITER[ \t]+(\S+)[ \t]*\r?\n?`)

// RemoveDelimiters removes DELIMITER statements from SQL and returns the clean SQL
// along with information about what delimiters were used.
// It also replaces custom delimiters at statement ends with semicolons.
func RemoveDelimiters(sql string) (cleanSQL string, delimiters []DelimiterInfo) {
	delimiters = []DelimiterInfo{}
	currentDelimiter := ";"
	result := strings.Builder{}

	// Find all DELIMITER statements
	matches := delimiterRegex.FindAllStringSubmatchIndex(sql, -1)
	if len(matches) == 0 {
		return sql, delimiters
	}

	lastEnd := 0
	for _, match := range matches {
		start := match[0]
		end := match[1]
		delimValue := sql[match[2]:match[3]]
		originalLine := sql[start:end]

		// Store delimiter info
		delimiters = append(delimiters, DelimiterInfo{
			OriginalLine: originalLine,
			Delimiter:    delimValue,
		})

		// Copy content from last end to this start, replacing custom delimiters with semicolons
		chunk := sql[lastEnd:start]
		if currentDelimiter != ";" {
			// Only replace the custom delimiter at statement boundaries, not inside strings
			chunk = replaceDelimiterOutsideStrings(chunk, currentDelimiter, ";")
		}
		result.WriteString(chunk)

		currentDelimiter = delimValue
		lastEnd = end
	}

	// Handle remaining content after last DELIMITER statement
	remaining := sql[lastEnd:]
	if currentDelimiter != ";" {
		remaining = replaceDelimiterOutsideStrings(remaining, currentDelimiter, ";")
	}
	result.WriteString(remaining)

	return result.String(), delimiters
}

// RestoreDelimiters wraps SQL with DELIMITER statements if custom delimiters were used.
// IMPORTANT: This does NOT replace semicolons inside procedure/function bodies.
// It only adds DELIMITER statements at the beginning and end.
func RestoreDelimiters(sql string, delimiters []DelimiterInfo) string {
	if len(delimiters) == 0 {
		return sql
	}

	// Find if there's a custom delimiter (not ";")
	customDelim := ""
	for _, d := range delimiters {
		if d.Delimiter != ";" {
			customDelim = d.Delimiter
			break
		}
	}

	if customDelim == "" {
		return sql
	}

	// Wrap with DELIMITER statements
	// Replace only the statement-ending semicolons (top-level), not those inside procedures
	return "DELIMITER " + customDelim + "\n" + replaceTopLevelSemicolons(sql, customDelim) + "\nDELIMITER ;\n"
}

// replaceDelimiterOutsideStrings replaces oldDelim with newDelim, but only outside of string literals.
func replaceDelimiterOutsideStrings(sql string, oldDelim string, newDelim string) string {
	var result strings.Builder
	inSingleQuote := false
	inDoubleQuote := false
	inBacktick := false

	oldDelimRunes := []rune(oldDelim)
	runes := []rune(sql)

	for i := 0; i < len(runes); i++ {
		ch := runes[i]

		// Check for escape sequences
		if ch == '\\' && i+1 < len(runes) {
			result.WriteRune(ch)
			i++
			result.WriteRune(runes[i])
			continue
		}

		// Track string state
		if ch == '\'' && !inDoubleQuote && !inBacktick {
			inSingleQuote = !inSingleQuote
		} else if ch == '"' && !inSingleQuote && !inBacktick {
			inDoubleQuote = !inDoubleQuote
		} else if ch == '`' && !inSingleQuote && !inDoubleQuote {
			inBacktick = !inBacktick
		}

		// Check for delimiter match outside strings
		if !inSingleQuote && !inDoubleQuote && !inBacktick {
			if i+len(oldDelimRunes) <= len(runes) {
				match := true
				for j, r := range oldDelimRunes {
					if runes[i+j] != r {
						match = false
						break
					}
				}
				if match {
					result.WriteString(newDelim)
					i += len(oldDelimRunes) - 1
					continue
				}
			}
		}

		result.WriteRune(ch)
	}

	return result.String()
}

// replaceTopLevelSemicolons replaces semicolons that end top-level statements with the custom delimiter.
// Semicolons inside BEGIN...END blocks are preserved.
func replaceTopLevelSemicolons(sql string, customDelim string) string {
	var result strings.Builder
	inSingleQuote := false
	inDoubleQuote := false
	inBacktick := false
	beginEndDepth := 0

	runes := []rune(sql)
	i := 0

	for i < len(runes) {
		ch := runes[i]

		// Check for escape sequences in strings
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

		// Skip string content
		if inSingleQuote || inDoubleQuote || inBacktick {
			result.WriteRune(ch)
			i++
			continue
		}

		// Check for BEGIN keyword (case insensitive)
		if matchKeyword(runes, i, "BEGIN") {
			beginEndDepth++
			result.WriteString("BEGIN")
			i += 5
			continue
		}

		// Check for END keyword (case insensitive)
		// Need to be careful not to match END IF, END WHILE, etc. as block ends
		if matchKeyword(runes, i, "END") {
			// Check if it's followed by IF, WHILE, LOOP, REPEAT, CASE (these don't decrease depth)
			endLen := 3
			afterEnd := i + 3
			// Skip whitespace
			for afterEnd < len(runes) && (runes[afterEnd] == ' ' || runes[afterEnd] == '\t' || runes[afterEnd] == '\n' || runes[afterEnd] == '\r') {
				afterEnd++
			}

			isBlockEnd := true
			if matchKeyword(runes, afterEnd, "IF") ||
			   matchKeyword(runes, afterEnd, "WHILE") ||
			   matchKeyword(runes, afterEnd, "LOOP") ||
			   matchKeyword(runes, afterEnd, "REPEAT") ||
			   matchKeyword(runes, afterEnd, "CASE") ||
			   matchKeyword(runes, afterEnd, "FOR") {
				isBlockEnd = false
			}

			if isBlockEnd && beginEndDepth > 0 {
				beginEndDepth--
			}
			result.WriteString("END")
			i += endLen
			continue
		}

		// Replace semicolons only at top level (outside BEGIN...END blocks)
		if ch == ';' {
			if beginEndDepth == 0 {
				result.WriteString(customDelim)
			} else {
				result.WriteRune(ch)
			}
			i++
			continue
		}

		result.WriteRune(ch)
		i++
	}

	return result.String()
}

// matchKeyword checks if the keyword appears at position i (case insensitive)
// and is followed by a non-alphanumeric character (word boundary)
func matchKeyword(runes []rune, i int, keyword string) bool {
	if i+len(keyword) > len(runes) {
		return false
	}

	// Check if preceded by alphanumeric (not a word start)
	if i > 0 {
		prev := runes[i-1]
		if isAlphanumeric(prev) || prev == '_' {
			return false
		}
	}

	for j, ch := range keyword {
		r := runes[i+j]
		// Case insensitive comparison
		if !((r == rune(ch)) || (r >= 'a' && r <= 'z' && r-32 == rune(ch)) || (r >= 'A' && r <= 'Z' && r == rune(ch))) {
			return false
		}
	}

	// Check word boundary after keyword
	afterPos := i + len(keyword)
	if afterPos < len(runes) {
		after := runes[afterPos]
		if isAlphanumeric(after) || after == '_' {
			return false
		}
	}

	return true
}

func isAlphanumeric(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}
