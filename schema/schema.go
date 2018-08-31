package schema

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"reflect"
	"strings"

	"github.com/dennwc/cas/types"
)

var ErrNotSchema = errors.New("not a schema file")

const (
	typeField = "@type"
	casNS     = "cas:"

	tab     = " "
	magic   = "{\n" + tab + "\"" + typeField + `":`
	maxSize = 16 * 1024 * 1024
)

const (
	MagicSize = len(magic)
)

type Object interface {
	References() []types.Ref
}

var (
	typesMap = make(map[string]reflect.Type)
)

func Register(o Object) {
	rt := reflect.TypeOf(o)
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	typesMap[rt.Name()] = rt
}

func TypeOf(o Object) (string, error) {
	rt := reflect.TypeOf(o)
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	t, ok := typesMap[rt.Name()]
	if !ok || t != rt {
		return "", fmt.Errorf("unsupported schema type: %T", o)
	}
	return casNS + rt.Name(), nil
}

func NewType(typ string) (Object, error) {
	if !strings.HasPrefix(typ, casNS) {
		return nil, fmt.Errorf("unsupported namespace: %q", typ)
	}
	typ = strings.TrimPrefix(typ, casNS)
	rt, ok := typesMap[typ]
	if !ok {
		return nil, fmt.Errorf("unsupported schema type: %q", typ)
	}
	return reflect.New(rt).Interface().(Object), nil
}

func Encode(w io.Writer, o Object) error {
	typ, err := TypeOf(o)
	if err != nil {
		return err
	}
	buf := new(bytes.Buffer)
	buf.WriteString(magic + ` "`)
	buf.WriteString(typ)
	buf.WriteString(`"`)
	i := buf.Len()

	enc := json.NewEncoder(buf)
	enc.SetIndent("", tab)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(o); err != nil {
		return fmt.Errorf("failed to encode %T: %v", o, err)
	}
	p := buf.Bytes()
	p[i] = ','
	_, err = w.Write(p)
	return err
}

// IsSchema checks if the buffer is likely to contain an object with a CAS schema.
// The buffer should be at least of MagicSize.
func IsSchema(p []byte) bool {
	if len(p) < MagicSize {
		return false
	}
	return string(p[:MagicSize]) == magic
}

func Decode(r io.Reader) (Object, error) {
	m := make([]byte, MagicSize)
	_, err := io.ReadFull(r, m)
	if err != nil {
		return nil, fmt.Errorf("cannot decode schema object: %v", err)
	}
	if !IsSchema(m) {
		return nil, ErrNotSchema
	}
	r = io.MultiReader(bytes.NewReader(m), r)
	obj, err := decode(r)
	if err != nil {
		return nil, fmt.Errorf("cannot decode schema object: %v", err)
	}
	return obj, nil
}

func decode(r io.Reader) (Object, error) {
	r = io.LimitReader(r, maxSize)
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	} else if len(data) == maxSize {
		return nil, fmt.Errorf("schema object is too large")
	}
	var h struct {
		Type string `json:"@type"`
	}
	if err := json.Unmarshal(data, &h); err != nil {
		return nil, err
	}
	obj, err := NewType(h.Type)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, obj); err != nil {
		return nil, err
	}
	return obj, nil
}
