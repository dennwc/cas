package types

import (
	"bytes"
	"crypto/sha256"
	"encoding"
	"encoding/base32"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"strings"
)

const (
	DefaultHash = hashSha256Name
)

const (
	useBase32      = false
	hashSha256Name = "sha256"
	hashBufSize    = sha256.Size
)

var refEnc = base32.StdEncoding.WithPadding(base32.NoPadding)

// IsRef checks if string is a text representation of a Ref.
func IsRef(s string) bool {
	return strings.HasPrefix(s, DefaultHash+":")
}

// ParseRef parses the string as a Ref.
func ParseRef(s string) (Ref, error) {
	return ParseRefBytes([]byte(s))
}

// ParseRef parses byte slice as a string representation of a Ref.
func ParseRefBytes(s []byte) (Ref, error) {
	if len(s) == 0 {
		return Ref{}, nil
	}
	i := bytes.Index(s, []byte(":"))
	if i < 0 {
		return Ref{}, fmt.Errorf("not a ref")
	}
	ref := Ref{
		name: string(s[:i]),
	}
	s = s[i+1:]
	sz := 0
	switch ref.name {
	case hashSha256Name:
		sz = sha256.Size
	default:
		return Ref{}, fmt.Errorf("unsupported ref type: %q", ref.name)
	}
	var dsz int
	if useBase32 {
		dsz = refEnc.DecodedLen(len(s))
	} else {
		dsz = hex.DecodedLen(len(s))
	}
	if dsz != sz {
		return Ref{}, fmt.Errorf("wrong size for %s ref: expected %d, got %d", ref.name, sz, dsz)
	}
	var (
		n   int
		err error
	)
	if useBase32 {
		n, err = refEnc.Decode(ref.data[:], s)
	} else {
		n, err = hex.Decode(ref.data[:], s)
	}
	if err != nil {
		return Ref{}, err
	} else if n != sz {
		return Ref{}, fmt.Errorf("wrong size for %s ref: expected %d, got %d", ref.name, sz, n)
	}
	return ref, nil
}

// MustParseRef is the same as ParseRef, but panics on error.
func MustParseRef(s string) Ref {
	ref, err := ParseRef(s)
	if err != nil {
		panic(err)
	}
	return ref
}

// NewRef creates a new zero ref with a default hash function.
//
// Example:
//	ref := NewRef()
//	h := ref.Hash()
//	h.Write(p)
//	ref = ref.WithHash(h)
func NewRef() Ref {
	return Ref{name: DefaultHash}
}

// MakeRef creates a ref with a specified hash algorithm and value.
func MakeRef(name string, data []byte) (Ref, error) {
	sz := 0
	switch name {
	case hashSha256Name:
		sz = sha256.Size
	default:
		return Ref{}, fmt.Errorf("unsupported ref type: %q", name)
	}
	if sz != len(data) {
		return Ref{}, fmt.Errorf("wrong size for %s ref: expected %d, got %d", name, sz, len(data))
	}
	r := Ref{name: name}
	copy(r.data[:], data)
	return r, nil
}

// BytesRef computes a Ref for a byte slice p.
func BytesRef(p []byte) Ref {
	ref := NewRef()
	h := ref.Hash()
	if _, err := h.Write(p); err != nil {
		panic(err)
	}
	return ref.WithHash(h)
}

// StringRef computes a Ref for a string s.
func StringRef(s string) Ref {
	return BytesRef([]byte(s))
}

// SizedRef is a object that combines a Ref and a size of a blob that it describes.
type SizedRef struct {
	Ref  Ref    `json:"ref"`
	Size uint64 `json:"size,omitempty"`
}

// DataBlob implements schema.BlobWrapper interface.
func (sr *SizedRef) DataBlob() Ref {
	return sr.Ref
}

// References implements schema.Object interface.
func (sr *SizedRef) References() []Ref {
	if sr.Ref.Zero() {
		return nil
	}
	return []Ref{sr.Ref}
}

// SchemaRef is a reference that describes a schema object.
// It stores the size of a schema object in bytes and the type of an object.
type SchemaRef struct {
	Ref  Ref    `json:"ref"`
	Size uint64 `json:"size,omitempty"`
	Type string `json:"type,omitempty"`
}

// SizedRef converts SchemaRef to SizedRef by omitting the type.
func (sr SchemaRef) SizedRef() SizedRef {
	return SizedRef{Ref: sr.Ref, Size: sr.Size}
}

// DataBlob implements schema.BlobWrapper interface.
func (sr *SchemaRef) DataBlob() Ref {
	return sr.Ref
}

// References implements schema.Object interface.
func (sr *SchemaRef) References() []Ref {
	if sr.Ref.Zero() {
		return nil
	}
	return []Ref{sr.Ref}
}

var (
	_ encoding.TextMarshaler   = Ref{}
	_ encoding.TextUnmarshaler = (*Ref)(nil)
)

var (
	emptyRef = BytesRef(nil)
)

// Ref is a reference to a blob in content-addressable storage.
// It consists of a hash type and the hash data. Refs are comparable.
type Ref struct {
	name string
	data [hashBufSize]byte
}

// MarshalText implements encoding.TextMarshaler interface.
func (r Ref) MarshalText() ([]byte, error) {
	return r.stringBytes(), nil
}

// UnmarshalText implements encoding.TextUnmarshaler interface.
func (r *Ref) UnmarshalText(s []byte) error {
	nr, err := ParseRefBytes(s)
	if err != nil {
		return err
	}
	*r = nr
	return nil
}

// Zero checks if a ref is not initialized.
func (r Ref) Zero() bool {
	return r == Ref{}
}

// Empty checks if this ref describes an empty blob (0 bytes).
func (r Ref) Empty() bool {
	return r.name == DefaultHash && r == emptyRef
}
func (r Ref) stringBytes() []byte {
	if r.Zero() {
		return nil
	}
	sz := len(r.name) + 1
	data := r.data[:]
	if useBase32 {
		sz += refEnc.EncodedLen(len(data))
	} else {
		sz += hex.EncodedLen(len(data))
	}
	buf := make([]byte, sz)
	i := copy(buf, r.name)
	buf[i] = ':'
	i++

	if useBase32 {
		refEnc.Encode(buf[i:], data)
	} else {
		hex.Encode(buf[i:], data)
	}
	return buf
}

// String returns a string representation of a ref.
func (r Ref) String() string {
	return string(r.stringBytes())
}

// GoString returns a ref representation suitable for the use in the Go source code.
func (r Ref) GoString() string {
	return fmt.Sprintf("types.MustParseRef(%q)", r.String())
}

// Name returns the name of the hash function used in this ref.
func (r Ref) Name() string {
	return r.name
}

// Data returns the hash value of this ref.
func (r Ref) Data() []byte {
	d := r.data
	return d[:]
}

// Hash initializes a new hash to populate the ref.
//
// Example:
//	h := ref.Hash()
//	h.Write(p)
//	ref = ref.WithHash(h)
func (r Ref) Hash() hash.Hash {
	switch r.name {
	case "":
		return nil
	case hashSha256Name:
		return sha256.New()
	default:
		panic(fmt.Errorf("hash with unknown type: %q", r.name))
	}
}

// WithHash returns a ref that is described by the specified hash.
func (r Ref) WithHash(h hash.Hash) Ref {
	_ = h.Sum(r.data[:0])
	return r
}

// Hash computes the ref for the specified reader.
func Hash(r io.Reader) (SizedRef, error) {
	ref := NewRef()
	h := ref.Hash()
	n, err := io.Copy(h, r)
	ref = ref.WithHash(h)
	return SizedRef{Ref: ref, Size: uint64(n)}, err
}

// Pin is a named reference to a blob.
type Pin struct {
	Name string `json:"name"`
	Ref  Ref    `json:"ref"`
}

// References implements schema.Object interface.
func (p *Pin) References() []Ref {
	if p.Ref.Zero() {
		return nil
	}
	return []Ref{p.Ref}
}
