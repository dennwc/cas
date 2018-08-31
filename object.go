package cas

import (
	"encoding/gob"
)

func init() {
	gob.RegisterName("Ref", Ref{})
	gob.RegisterName("SizedRef", SizedRef{})
	gob.RegisterName("Concat", Concat{})
}

type Concat struct {
	ElemType string     `json:"etype,omitempty"`
	Parts    []SizedRef `json:"parts"`
}
