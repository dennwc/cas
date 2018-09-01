package schema

import "github.com/dennwc/cas/types"

func init() {
	Register(&Directory{})
	Register(&JoinDirectories{})
	Register(&DirEntry{})
	Register(&Compressed{})
	Register(&Multipart{})
}

type Directory struct {
	List []DirEntry `json:"list"`
}

func (d *Directory) References() []types.Ref {
	refs := make([]types.Ref, 0, len(d.List))
	for _, e := range d.List {
		refs = append(refs, e.Ref)
	}
	return refs
}

type JoinDirectories struct {
	Count uint        `json:"cnt"`
	Size  uint64      `json:"size"`
	List  []types.Ref `json:"list"`
}

func (d *JoinDirectories) References() []types.Ref {
	return append([]types.Ref{}, d.List...)
}

type DirEntry struct {
	Ref   types.Ref `json:"ref"`
	Name  string    `json:"name"`
	Count uint      `json:"cnt,omitempty"`  // count of files inside this dir entry (not counting this entry)
	Size  uint64    `json:"size,omitempty"` // total size of files inside this dir entry or a size of the file
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
