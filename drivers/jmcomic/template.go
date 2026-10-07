package jmcomic

import (
	"encoding/json"
	"errors"
	"strings"
)

func templateSpace(s string, i int) (int, error) {
	for i < len(s) {
		switch s[i] {
		case ' ', '\t', '\r', '\n':
			i++
			continue
		}
		if strings.HasPrefix(s[i:], "//") {
			for i < len(s) && s[i] != '\n' {
				i++
			}
			continue
		}
		if strings.HasPrefix(s[i:], "/*") {
			end := strings.Index(s[i+2:], "*/")
			if end < 0 {
				return i, errors.New("unterminated JMComic template comment")
			}
			i += end + 4
			continue
		}
		break
	}
	return i, nil
}

func templateIdentifier(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_' || c == '$'
}

// Convert literal syntax only; no JavaScript expressions or scripts are run.
// The upstream uses bare keys, single quotes and trailing array commas.
func templateJSON(s string) ([]byte, error) {
	var out strings.Builder
	for i := 0; i < len(s); {
		next, err := templateSpace(s, i)
		if err != nil {
			return nil, err
		}
		if next != i {
			out.WriteByte(' ')
			i = next
			continue
		}
		c := s[i]
		if c == '\'' || c == '"' {
			quote := c
			out.WriteByte('"')
			i++
			closed := false
			for i < len(s) {
				c = s[i]
				i++
				if c == quote {
					out.WriteByte('"')
					closed = true
					break
				}
				if c == '"' {
					out.WriteString(`\"`)
				} else if c != '\\' {
					out.WriteByte(c)
				} else {
					if i == len(s) {
						return nil, errors.New("unterminated JMComic template escape")
					}
					c = s[i]
					i++
					switch c {
					case '\'':
						out.WriteByte('\'')
					case '"', '\\', '/', 'b', 'f', 'n', 'r', 't', 'u':
						out.WriteByte('\\')
						out.WriteByte(c)
					case 'x':
						if i+2 > len(s) {
							return nil, errors.New("invalid JMComic template hex escape")
						}
						out.WriteString(`\u00` + s[i:i+2])
						i += 2
					default:
						return nil, errors.New("unsupported JMComic template escape")
					}
				}
			}
			if !closed {
				return nil, errors.New("unterminated JMComic template string")
			}
			continue
		}
		if templateIdentifier(c) {
			end := i + 1
			for end < len(s) && (templateIdentifier(s[end]) || s[end] >= '0' && s[end] <= '9') {
				end++
			}
			next, err := templateSpace(s, end)
			if err != nil {
				return nil, err
			}
			if next < len(s) && s[next] == ':' {
				key, _ := json.Marshal(s[i:end])
				out.Write(key)
			} else {
				out.WriteString(s[i:end])
			}
			i = end
			continue
		}
		if c == ',' {
			next, err := templateSpace(s, i+1)
			if err != nil {
				return nil, err
			}
			if next < len(s) && (s[next] == '}' || s[next] == ']') {
				i++
				continue
			}
		}
		out.WriteByte(c)
		i++
	}
	return []byte(out.String()), nil
}

func decodeTemplateObject(s string, dst any) error {
	b, err := templateJSON(s)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, dst)
}
