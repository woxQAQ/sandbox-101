// sqlfmt expands CREATE statements in SQLite migrations so each field sits on
// its own line, with one blank line between statements.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

// split cuts src into statements at top-level semicolons. Parentheses must
// balance and strings must terminate, so a malformed migration fails instead
// of being rewritten incorrectly. Empty statements are dropped.
func split(src string) ([]string, error) {
	var statements []string
	start, depth := 0, 0
	inString := false
	for index := 0; index < len(src); index++ {
		if src[index] == '\'' {
			if inString && index+1 < len(src) && src[index+1] == '\'' {
				index++ // Escaped quote inside a string literal.
				continue
			}
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		switch src[index] {
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return nil, fmt.Errorf("unbalanced ')' at byte %d", index)
			}
		case ';':
			if depth == 0 {
				if statement := strings.TrimSpace(src[start:index]); statement != "" {
					statements = append(statements, statement)
				}
				start = index + 1
			}
		}
	}
	if inString {
		return nil, fmt.Errorf("unterminated string literal")
	}
	if depth != 0 {
		return nil, fmt.Errorf("unbalanced '('")
	}
	if tail := strings.TrimSpace(src[start:]); tail != "" {
		return nil, fmt.Errorf("statement %q is not terminated by ';'", tail)
	}
	return statements, nil
}

// format expands the first parenthesized list of statement into one item per
// line. Lists with a single item stay inline, but the parenthesis layout is
// still normalized. The returned text includes the terminating semicolon.
func format(statement string) (string, error) {
	open := strings.IndexByte(statement, '(')
	if open < 0 {
		return statement + ";", nil
	}
	depth, inString, close := 1, false, -1
	for index := open + 1; index < len(statement); index++ {
		if statement[index] == '\'' {
			if inString && index+1 < len(statement) && statement[index+1] == '\'' {
				index++
				continue
			}
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		switch statement[index] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				close = index
			}
		}
		if close >= 0 {
			break
		}
	}
	if close < 0 {
		return "", fmt.Errorf("unbalanced '(' in %q", statement)
	}
	items, err := items(statement[open+1 : close])
	if err != nil {
		return "", err
	}
	prefix := strings.TrimSpace(statement[:open])
	suffix := strings.TrimSpace(statement[close+1:])
	var text string
	switch len(items) {
	case 0:
		// An empty list is not valid SQL; pass the statement through.
		return statement + ";", nil
	case 1:
		text = prefix + " (" + items[0] + ")"
	default:
		text = prefix + " (\n\t" + strings.Join(items, ",\n\t") + "\n)"
	}
	if suffix != "" {
		text += " " + suffix
	}
	return text + ";", nil
}

// items splits src at top-level commas and trims each item. Commas nested in
// parentheses or string literals do not separate items.
func items(src string) ([]string, error) {
	var items []string
	start, depth := 0, 0
	inString := false
	for index := 0; index < len(src); index++ {
		if src[index] == '\'' {
			if inString && index+1 < len(src) && src[index+1] == '\'' {
				index++
				continue
			}
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		switch src[index] {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				item := strings.TrimSpace(src[start:index])
				if item == "" {
					return nil, fmt.Errorf("empty item in list %q", src)
				}
				items = append(items, item)
				start = index + 1
			}
		}
	}
	if last := strings.TrimSpace(src[start:]); last != "" {
		items = append(items, last)
	}
	return items, nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	write := flag.Bool("w", false, "write formatted files in place")
	flag.Parse()
	if flag.NArg() == 0 {
		return fmt.Errorf("usage: go tool sqlfmt [-w] FILE [FILE ...]")
	}
	for _, name := range flag.Args() {
		source, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		statements, err := split(string(source))
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		formatted := make([]string, 0, len(statements))
		for _, statement := range statements {
			text, err := format(statement)
			if err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
			formatted = append(formatted, text)
		}
		output := strings.Join(formatted, "\n\n") + "\n"
		if *write {
			if err = os.WriteFile(name, []byte(output), 0o600); err != nil {
				return err
			}
		} else if _, err = os.Stdout.WriteString(output); err != nil {
			return err
		}
	}
	return nil
}
