package schema

import "github.com/dennwc/cas/types"

func init() {
	Register(&DirEntry{})
	Register(&Compressed{})
	Register(&Multipart{})
}

type DirEntry struct {
	Ref   types.Ref `json:"ref"`
	Name  string    `json:"name"`
	Stats Stats     `json:"stats"`
}

func (d *DirEntry) Size() uint64 {
	return d.Stats.Size()
}

func (d *DirEntry) References() []types.Ref {
	return []types.Ref{d.Ref}
}

type Compressed struct {
	Algo string         `json:"algo"`
	Arch types.SizedRef `json:"arch"`
	Ref  types.SizedRef `json:"ref"`
}

func (c *Compressed) References() []types.Ref {
	return []types.Ref{c.Arch.Ref, c.Ref.Ref}
}

type Multipart struct {
	Ref   types.Ref        `json:"ref,omitempty"`
	Parts []types.SizedRef `json:"parts"`
}

func (m *Multipart) References() []types.Ref {
	refs := make([]types.Ref, 0, len(m.Parts)+1)
	refs = append(refs, m.Ref)
	for _, sr := range m.Parts {
		refs = append(refs, sr.Ref)
	}
	return refs
}
