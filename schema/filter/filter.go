package filter

import (
	"go/ast"
	"go/parser"
)

type Filter interface {
	FilterObject(o interface{}) (bool, error)
}

// Compile the filter expression.
func Compile(expr string) (Filter, error) {
	x, err := parser.ParseExpr(expr)
	if err != nil {
		return nil, err
	}
	return newFilterFromAST(x), nil
}

func newFilterFromAST(x ast.Expr) Filter {
	return filter{x: x}
}

type filter struct {
	x ast.Expr
}

func (f filter) FilterObject(o interface{}) (bool, error) {
	vm := newVM(map[string]interface{}{
		"s": o,
	})
	v, err := vm.Eval(f.x)
	if err != nil {
		return false, err
	}
	ok, _ := v.(bool)
	return ok, nil
}
