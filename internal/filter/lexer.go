package filter

import (
	"fmt"
	"strings"
	"unicode"
)

type tokenType int

const (
	tokenEOF tokenType = iota
	tokenString
	tokenIdent
	tokenAnd
	tokenOr
	tokenNot
	tokenEq
	tokenNeq
	tokenColon
	tokenLParen
	tokenRParen
	tokenComma
)

type token struct {
	typ tokenType
	val string
}

type lexer struct {
	input  string
	pos    int
	tokens []token
}

func lex(input string) ([]token, error) {
	l := &lexer{input: input}
	if err := l.run(); err != nil {
		return nil, err
	}
	return l.tokens, nil
}

func (l *lexer) run() error {
	for l.pos < len(l.input) {
		ch := l.input[l.pos]

		switch {
		case ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r':
			l.pos++

		case ch == '"':
			tok, err := l.readString()
			if err != nil {
				return err
			}
			l.tokens = append(l.tokens, tok)

		case ch == '(':
			l.tokens = append(l.tokens, token{tokenLParen, "("})
			l.pos++

		case ch == ')':
			l.tokens = append(l.tokens, token{tokenRParen, ")"})
			l.pos++

		case ch == ',':
			l.tokens = append(l.tokens, token{tokenComma, ","})
			l.pos++

		case ch == ':':
			l.tokens = append(l.tokens, token{tokenColon, ":"})
			l.pos++

		case ch == '=' :
			l.tokens = append(l.tokens, token{tokenEq, "="})
			l.pos++

		case ch == '!' && l.pos+1 < len(l.input) && l.input[l.pos+1] == '=':
			l.tokens = append(l.tokens, token{tokenNeq, "!="})
			l.pos += 2

		case isIdentStart(ch):
			tok := l.readIdent()
			switch tok.val {
			case "AND":
				tok.typ = tokenAnd
			case "OR":
				tok.typ = tokenOr
			case "NOT":
				tok.typ = tokenNot
			}
			l.tokens = append(l.tokens, tok)

		default:
			return fmt.Errorf("unexpected character %q at position %d", ch, l.pos)
		}
	}
	l.tokens = append(l.tokens, token{tokenEOF, ""})
	return nil
}

func (l *lexer) readString() (token, error) {
	l.pos++ // skip opening quote
	var b strings.Builder
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == '\\' && l.pos+1 < len(l.input) {
			l.pos++
			b.WriteByte(l.input[l.pos])
			l.pos++
			continue
		}
		if ch == '"' {
			l.pos++
			return token{tokenString, b.String()}, nil
		}
		b.WriteByte(ch)
		l.pos++
	}
	return token{}, fmt.Errorf("unterminated string starting at position %d", l.pos)
}

func (l *lexer) readIdent() token {
	start := l.pos
	for l.pos < len(l.input) && isIdentPart(l.input[l.pos]) {
		l.pos++
	}
	return token{tokenIdent, l.input[start:l.pos]}
}

func isIdentStart(ch byte) bool {
	return ch == '_' || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func isIdentPart(ch byte) bool {
	r := rune(ch)
	return unicode.IsLetter(r) || unicode.IsDigit(r) || ch == '_' || ch == '.'
}
