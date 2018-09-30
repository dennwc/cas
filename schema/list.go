package schema

import (
	"encoding/json"

	"github.com/dennwc/cas/types"
)

func init() {
	registerCAS(&List{})
	registerCAS(&InlineList{})
}

// List is an ordered list of entries of a specific type.
type List struct {
	Ref   *types.Ref  `json:"ref,omitempty"`   // ref of concatenated content, if applicable
	Elem  string      `json:"elem,omitempty"`  // type of elements in List
	List  []types.Ref `json:"list,omitempty"`  // List<Elem> or InlineList<Elem>
	Stats Stats       `json:"stats,omitempty"` // optional stats
}

func (l *List) References() []types.Ref {
	return append([]types.Ref{}, l.List...)
}

var _ json.Unmarshaler = (*InlineList)(nil)

// InlineList is an inlined list of entries of a specific type.
type InlineList struct {
	Ref   *types.Ref `json:"ref,omitempty"`   // ref of concatenated content, if applicable
	Elem  string     `json:"elem,omitempty"`  // type of elements in List
	List  []Object   `json:"list,omitempty"`  // Elem
	Stats Stats      `json:"stats,omitempty"` // optional stats
}

func (l *InlineList) UnmarshalJSON(p []byte) error {
	var list struct {
		Ref   *types.Ref        `json:"ref"`
		Elem  string            `json:"elem"`
		List  []json.RawMessage `json:"list"`
		Stats Stats             `json:"stats"`
	}
	if err := json.Unmarshal(p, &list); err != nil {
		return err
	}
	l.Ref, l.Elem, l.Stats = list.Ref, list.Elem, list.Stats
	l.List = make([]Object, 0, len(list.List))
	for _, edata := range list.List {
		v, err := NewType(list.Elem)
		if err != nil {
			return err
		}
		if err = json.Unmarshal(edata, v); err != nil {
			return err
		}
		l.List = append(l.List, v)
	}
	return nil
}

func (l *InlineList) References() []types.Ref {
	var out []types.Ref
	for _, e := range l.List {
		out = append(out, e.References()...)
	}
	return out
}
