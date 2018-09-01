package cas

import (
	"context"

	"github.com/dennwc/cas/storage"
)

type SchemaIterator = storage.SchemaIterator

func (s *Storage) IterateSchema(ctx context.Context, typs ...string) SchemaIterator {
	return s.st.IterateSchema(ctx, typs...)
}

func (s *Storage) Reindex(ctx context.Context, force bool) error {
	return s.st.Reindex(ctx, force)
}
