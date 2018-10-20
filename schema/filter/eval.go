package filter

import (
	"fmt"
	"go/ast"
	"go/token"
	"reflect"
	"strconv"
	"strings"
)

type vm struct {
	globals map[string]interface{}
}

func newVM(globals map[string]interface{}) *vm {
	if globals == nil {
		globals = make(map[string]interface{}, 4)
	}
	globals["nil"] = nil
	globals["true"] = true
	globals["false"] = false
	return &vm{
		globals: globals,
	}
}

func (vm *vm) Eval(x ast.Expr) (interface{}, error) {
	switch x := x.(type) {
	case *ast.BasicLit:
		switch x.Kind {
		case token.STRING:
			v, err := strconv.Unquote(x.Value)
			if err != nil {
				return nil, err
			}
			return v, nil
		case token.INT:
			// TODO: support hex and oct
			v, err := strconv.ParseInt(x.Value, 10, 64)
			if err != nil {
				return nil, err
			}
			return intType(v), nil
		default:
			return nil, fmt.Errorf("unexpected literal type: %v", x.Kind)
		}
	case *ast.Ident:
		v, ok := vm.globals[x.Name]
		if !ok {
			return nil, fmt.Errorf("undefined identifier: %q", x.Name)
		}
		return v, nil
	case *ast.BinaryExpr:
		left, err := vm.Eval(x.X)
		if err != nil {
			return nil, err
		}
		right, err := vm.Eval(x.Y)
		if err != nil {
			return nil, err
		}
		return vm.evalBinOp(left, x.Op, right)
	case *ast.SelectorExpr:
		o, err := vm.Eval(x.X)
		if err != nil {
			return nil, err
		} else if o == nil {
			return nil, nil
		}
		name := x.Sel.Name
		if m, ok := o.(map[string]interface{}); ok {
			v := m[name]
			return v, nil
		}
		rv := reflect.ValueOf(o)
		if rv.Kind() == reflect.Ptr {
			rv = rv.Elem()
		}
		if rv.Kind() != reflect.Struct {
			return nil, fmt.Errorf("cannot select on type: %v", rv.Type())
		}
		fv := rv.FieldByName(name)
		if fv.IsValid() {
			return fv.Interface(), nil
		}
		rt := rv.Type()
		for i := 0; i < rt.NumField(); i++ {
			f := rt.Field(i)
			tag, ok := f.Tag.Lookup("json")
			if !ok {
				continue
			}
			if i := strings.Index(tag, ","); i >= 0 {
				tag = tag[:i]
			}
			if tag == name {
				return rv.Field(i).Interface(), nil
			}
		}
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported expression: %T", x)
	}
}

func (vm *vm) evalBinOp(left interface{}, op token.Token, right interface{}) (interface{}, error) {
	left, right = alignTypes(left, right)
	switch op {
	case token.EQL:
		return left == right, nil
	case token.NEQ:
		return left != right, nil
	case token.OR, token.AND:
		x, _ := left.(bool)
		y, _ := right.(bool)
		switch op {
		case token.OR:
			return x || y, nil
		case token.AND:
			return x && y, nil
		}
	}
	x, ok := left.(intType)
	if !ok {
		return false, nil
	}
	y, ok := right.(intType)
	if !ok {
		return false, nil
	}
	switch op {
	case token.QUO:
		return x / y, nil
	case token.REM:
		return x % y, nil
	case token.GTR:
		return x > y, nil
	case token.LSS:
		return x < y, nil
	case token.GEQ:
		return x >= y, nil
	case token.LEQ:
		return x <= y, nil
	default:
		return nil, fmt.Errorf("unsupported operator: %v", op)
	}
}

type intType = int64

func simplifyType(a interface{}) interface{} {
	switch a := a.(type) {
	case int:
		return intType(a)
	case uint:
		return intType(a)
	case int64:
		return intType(a)
	case uint64:
		return intType(a)
	case float64:
		if float64(int64(a)) == a {
			return intType(a)
		}
	case int8, int16, int32, uint8, uint16, uint32:
		return reflect.ValueOf(a).Convert(reflect.TypeOf(intType(0))).Interface()
	}
	return a
}

func alignTypes(a, b interface{}) (interface{}, interface{}) {
	return simplifyType(a), simplifyType(b)
}
