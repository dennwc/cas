package schema

import (
	"time"

	"github.com/dennwc/cas/types"
)

func init() {
	registerCAS(&WebContent{})
}

var _ BlobWrapper = (*WebContent)(nil)

type WebContent struct {
	URL  string     `json:"url"`
	Ref  types.Ref  `json:"ref"`
	Size uint64     `json:"size,omitempty"`
	ETag string     `json:"etag,omitempty"`
	TS   *time.Time `json:"ts,omitempty"`
}

func (c *WebContent) DataBlob() types.Ref {
	return c.Ref
}

func (c *WebContent) References() []types.Ref {
	return []types.Ref{c.Ref}
}
