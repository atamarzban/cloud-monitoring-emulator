package filter

import "fmt"

// Parse parses a monitoring filter expression into an AST.
// Returns nil if the input is empty.
func Parse(input string) (Expr, error) {
	input = trimSpace(input)
	if input == "" {
		return nil, nil
	}

	tokens, err := lex(input)
	if err != nil {
		return nil, err
	}

	p := &parser{tokens: tokens}
	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	if p.tokens[p.pos].typ != tokenEOF {
		return nil, fmt.Errorf("unexpected token %q at position %d", p.tokens[p.pos].val, p.pos)
	}
	return expr, nil
}

type parser struct {
	tokens []token
	pos    int
}

func (p *parser) peek() token {
	return p.tokens[p.pos]
}

func (p *parser) advance() token {
	t := p.tokens[p.pos]
	p.pos++
	return t
}

func (p *parser) expect(typ tokenType) (token, error) {
	t := p.advance()
	if t.typ != typ {
		return t, fmt.Errorf("expected token type %d, got %q", typ, t.val)
	}
	return t, nil
}

// expr = andExpr
func (p *parser) parseExpr() (Expr, error) {
	return p.parseAndExpr()
}

// andExpr = orExpr ("AND" orExpr)*
func (p *parser) parseAndExpr() (Expr, error) {
	left, err := p.parseOrExpr()
	if err != nil {
		return nil, err
	}
	for p.peek().typ == tokenAnd {
		p.advance()
		right, err := p.parseOrExpr()
		if err != nil {
			return nil, err
		}
		left = &AndExpr{Left: left, Right: right}
	}
	return left, nil
}

// orExpr = unary ("OR" unary)*
func (p *parser) parseOrExpr() (Expr, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for p.peek().typ == tokenOr {
		p.advance()
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		left = &OrExpr{Left: left, Right: right}
	}
	return left, nil
}

// unary = "NOT" unary | primary
func (p *parser) parseUnary() (Expr, error) {
	if p.peek().typ == tokenNot {
		p.advance()
		inner, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return &NotExpr{Inner: inner}, nil
	}
	return p.parsePrimary()
}

// primary = comparison | "(" expr ")"
func (p *parser) parsePrimary() (Expr, error) {
	if p.peek().typ == tokenLParen {
		p.advance()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tokenRParen); err != nil {
			return nil, fmt.Errorf("expected closing parenthesis")
		}
		return expr, nil
	}
	return p.parseComparison()
}

// comparison = field op value
// field = IDENT
// op = "=" | "!=" | ":"
// value = STRING | functionCall
func (p *parser) parseComparison() (Expr, error) {
	fieldTok, err := p.expect(tokenIdent)
	if err != nil {
		return nil, fmt.Errorf("expected field name, got %q", fieldTok.val)
	}

	var op ComparisonOp
	switch p.peek().typ {
	case tokenEq:
		op = OpEq
		p.advance()
	case tokenNeq:
		op = OpNeq
		p.advance()
	case tokenColon:
		op = OpHas
		p.advance()
	default:
		return nil, fmt.Errorf("expected operator (=, !=, :), got %q", p.peek().val)
	}

	val, err := p.parseValue()
	if err != nil {
		return nil, err
	}

	return &ComparisonExpr{
		Field: fieldTok.val,
		Op:    op,
		Value: val,
	}, nil
}

// value = STRING | functionCall
// functionCall = IDENT "(" argList ")"
// argList = STRING ("," STRING)*
func (p *parser) parseValue() (Value, error) {
	if p.peek().typ == tokenString {
		t := p.advance()
		return &StringValue{Val: t.val}, nil
	}

	if p.peek().typ == tokenIdent {
		nameTok := p.advance()
		if p.peek().typ != tokenLParen {
			return nil, fmt.Errorf("expected '(' after function name %q or a string value", nameTok.val)
		}
		p.advance() // consume '('

		var args []string
		for p.peek().typ != tokenRParen {
			if len(args) > 0 {
				if _, err := p.expect(tokenComma); err != nil {
					return nil, fmt.Errorf("expected ',' between function arguments")
				}
			}
			argTok, err := p.expect(tokenString)
			if err != nil {
				return nil, fmt.Errorf("expected string argument in function %q", nameTok.val)
			}
			args = append(args, argTok.val)
		}
		p.advance() // consume ')'

		return &FunctionCall{Name: nameTok.val, Args: args}, nil
	}

	return nil, fmt.Errorf("expected string value or function call, got %q", p.peek().val)
}

func trimSpace(s string) string {
	i := 0
	for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r') {
		i++
	}
	j := len(s)
	for j > i && (s[j-1] == ' ' || s[j-1] == '\t' || s[j-1] == '\n' || s[j-1] == '\r') {
		j--
	}
	return s[i:j]
}
