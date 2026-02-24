package filter

// Expr is a node in the filter expression AST.
type Expr interface {
	expr()
}

// AndExpr represents a logical AND of two expressions.
type AndExpr struct {
	Left, Right Expr
}

func (*AndExpr) expr() {}

// OrExpr represents a logical OR of two expressions.
type OrExpr struct {
	Left, Right Expr
}

func (*OrExpr) expr() {}

// NotExpr represents a logical NOT of an expression.
type NotExpr struct {
	Inner Expr
}

func (*NotExpr) expr() {}

// ComparisonOp is the type of comparison in a filter expression.
type ComparisonOp int

const (
	OpEq  ComparisonOp = iota // =
	OpNeq                     // !=
	OpHas                     // : (substring/existence check)
)

// ComparisonExpr represents a field comparison like `metric.type = "foo"`.
type ComparisonExpr struct {
	Field string       // dot-separated path, e.g. "metric.type"
	Op    ComparisonOp // =, !=, :
	Value Value        // the right-hand side
}

func (*ComparisonExpr) expr() {}

// Value is the right-hand side of a comparison.
type Value interface {
	value()
}

// StringValue is a literal string value.
type StringValue struct {
	Val string
}

func (*StringValue) value() {}

// FunctionCall is a function invocation like starts_with("prefix").
type FunctionCall struct {
	Name string   // e.g. "starts_with", "one_of", "monitoring.regex.full_match"
	Args []string // string arguments
}

func (*FunctionCall) value() {}
