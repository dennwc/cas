package schema

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"reflect"

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

func init() {
	registerCAS(&types.SizedRef{})
	registerCAS(&types.SchemaRef{})
	registerCAS(&types.Pin{})
}

var _ BlobWrapper = (*types.SizedRef)(nil)

const (
	StatDataSize  = "size" // size of all raw blobs (excluding the schema)
	StatDataCount = "cnt"  // number of full objects (logical trees)
)

// Stats is a collection of stat values.
type Stats map[string]uint64

func (s Stats) Size() uint64 {
	return s[StatDataSize]
}

type Object interface {
	// TODO: split into DependsOn and Describes

	References() []types.Ref
}

type BlobWrapper interface {
	DataBlob() types.Ref
}

var (
	typesMap   = make(map[string]reflect.Type)
	typeToName = make(map[reflect.Type]string)
)

func registerCAS(o Object) {
	rt := reflect.TypeOf(o)
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	name := casNS + rt.Name()
	typesMap[name] = rt
	typeToName[rt] = name
}

func RegisterName(name string, o Object) {
	rt := reflect.TypeOf(o)
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	typesMap[name] = rt
	typeToName[rt] = name
}

func TypeOf(o Object) (string, error) {
	rt := reflect.TypeOf(o)
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	name, ok := typeToName[rt]
	if !ok {
		return "", fmt.Errorf("unsupported schema type: %T", o)
	}
	return name, nil
}

func MustTypeOf(o Object) string {
	typ, err := TypeOf(o)
	if err != nil {
		panic(err)
	}
	return typ
}

func NewType(typ string) (Object, error) {
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

func checkSchema(r io.Reader) (io.Reader, error) {
	m := make([]byte, MagicSize)
	_, err := io.ReadFull(r, m)
	if err != nil {
		return nil, fmt.Errorf("cannot check schema object: %v", err)
	}
	if !IsSchema(m) {
		return nil, ErrNotSchema
	}
	return io.MultiReader(bytes.NewReader(m), r), nil
}

func Decode(r io.Reader) (Object, error) {
	var err error
	r, err = checkSchema(r)
	if err != nil {
		return nil, err
	}
	obj, err := decode(r)
	if err != nil {
		return nil, fmt.Errorf("cannot decode schema object: %v", err)
	}
	return obj, nil
}

func decode(r io.Reader) (Object, error) {
	typ, data, err := decodeType(r)
	if err != nil {
		return nil, err
	}
	obj, err := NewType(typ)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func decodeType(r io.Reader) (string, []byte, error) {
	r = io.LimitReader(r, maxSize)
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return "", nil, err
	} else if len(data) == maxSize {
		return "", nil, fmt.Errorf("schema object is too large")
	}
	var h struct {
		Type string `json:"@type"`
	}
	if err := json.Unmarshal(data, &h); err != nil {
		return "", nil, err
	}
	return h.Type, data, nil
}

func DecodeType(r io.Reader) (string, error) {
	var err error
	r, err = checkSchema(r)
	if err != nil {
		return "", err
	}
	typ, _, err := decodeType(r)
	return typ, err
}
