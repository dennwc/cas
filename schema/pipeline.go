package schema

import "github.com/dennwc/cas/types"

func init() {
	Register(&TransformOp{})
}

type TransformOp struct {
	Src types.Ref `json:"src"`
	Op  types.Ref `json:"op"`
	Dst types.Ref `json:"dst"`
}

func (t *TransformOp) References() []types.Ref {
	return []types.Ref{t.Src, t.Op, t.Dst}
}
