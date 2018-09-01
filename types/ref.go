package types

import (
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

func hashTextEncode(h []byte) string {
	if useBase32 {
		return refEnc.EncodeToString(h)
	}
	return hex.EncodeToString(h)
}

func hashTextDecode(s string) ([]byte, error) {
	if useBase32 {
		return refEnc.DecodeString(s)
	}
	return hex.DecodeString(s)
}

func ParseRef(s string) (Ref, error) {
	i := strings.Index(s, ":")
	if i < 0 {
		return Ref{}, fmt.Errorf("not a ref")
	}
	ref := Ref{
		name: s[:i],
	}
	sz := 0
	switch ref.name {
	case hashSha256Name:
		sz = sha256.Size
	default:
		return Ref{}, fmt.Errorf("unsupported ref type: %q", ref.name)
	}
	data, err := hashTextDecode(s[i+1:])
	if err != nil {
		return Ref{}, err
	}
	n := copy(ref.data[:], data)
	if n != sz {
		return Ref{}, fmt.Errorf("wrong size for %s ref: expected %d, got %d", ref.name, sz, n)
	}
	return ref, nil
}

func MustParseRef(s string) Ref {
	ref, err := ParseRef(s)
	if err != nil {
		panic(err)
	}
	return ref
}

func NewRef() Ref {
	return Ref{name: DefaultHash}
}

func BytesRef(p []byte) Ref {
	ref := NewRef()
	h := ref.Hash()
	if _, err := h.Write(p); err != nil {
		panic(err)
	}
	return ref.WithHash(h)
}

func StringRef(s string) Ref {
	return BytesRef([]byte(s))
}

type SizedRef struct {
	Ref  Ref    `json:"ref"`
	Size uint64 `json:"size,omitempty"`
}

var (
	_ encoding.TextMarshaler   = Ref{}
	_ encoding.TextUnmarshaler = (*Ref)(nil)
)

var (
	emptyRef = BytesRef(nil)
)

type Ref struct {
	name string
	data [hashBufSize]byte
}

func (r Ref) MarshalText() ([]byte, error) {
	return []byte(r.String()), nil
}

func (r *Ref) UnmarshalText(s []byte) error {
	nr, err := ParseRef(string(s))
	if err != nil {
		return err
	}
	*r = nr
	return nil
}

func (r Ref) Zero() bool {
	return r == Ref{}
}
func (r Ref) Empty() bool {
	return r.name == DefaultHash && r == emptyRef
}
func (r Ref) String() string {
	return r.name + ":" + hashTextEncode(r.data[:])
}
func (r Ref) GoString() string {
	return fmt.Sprintf("types.MustParseRef(%q)", r.String())
}
func (r Ref) Name() string {
	return r.name
}
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
func (r Ref) WithHash(h hash.Hash) Ref {
	_ = h.Sum(r.data[:0])
	return r
}

func Hash(r io.Reader) (SizedRef, error) {
	ref := NewRef()
	h := ref.Hash()
	n, err := io.Copy(h, r)
	ref = ref.WithHash(h)
	return SizedRef{Ref: ref, Size: uint64(n)}, err
}
