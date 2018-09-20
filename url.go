package cas

import (
	"context"
	"net/url"

	"github.com/dennwc/cas/types"
)

// StoreAddr interprets an address as either a local FS path or URL and fetches the content.
// It will create schema objects automatically.
func (s *Storage) StoreAddr(ctx context.Context, addr string, indexOnly bool) (types.SizedRef, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return types.SizedRef{}, err
	}
	if indexOnly {
		if u.Scheme != "" {
			return s.IndexURLContent(ctx, addr)
		}
		return s.IndexFilePath(ctx, addr)
	}
	if u.Scheme != "" {
		return s.StoreURLContent(ctx, addr)
	}
	return s.StoreFilePath(ctx, addr)
}
