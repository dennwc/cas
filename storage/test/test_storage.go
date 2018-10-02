package storagetest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"github.com/dennwc/cas/storage"
	"github.com/dennwc/cas/types"
)

type StorageFunc func(t testing.TB) (storage.Storage, func())

func RunTests(t *testing.T, fnc StorageFunc) {
	t.Run("simple", func(t *testing.T) {
		testSimple(t, fnc)
	})
}

func testSimple(t *testing.T, fnc StorageFunc) {
	s, closer := fnc(t)
	defer closer()

	ctx := context.Background()
	w, err := s.BeginBlob(ctx)
	require.NoError(t, err)
	defer w.Close()

	data := []byte("useful data")
	expRef := types.SizedRef{
		Ref: types.BytesRef(data), Size: uint64(len(data)),
	}

	n, err := w.Write(data)
	require.NoError(t, err)
	require.Equal(t, int(len(data)), n)

	sz := w.Size()
	require.Equal(t, uint64(len(data)), sz)

	sr, err := w.Complete()
	require.NoError(t, err)
	require.Equal(t, expRef, sr)

	err = w.Commit()
	require.NoError(t, err)
	err = w.Close()
	assert.Equal(t, storage.ErrBlobCompleted, err)

	sz, err = s.StatBlob(ctx, sr.Ref)
	require.NoError(t, err)
	require.Equal(t, uint64(len(data)), sz)

	rc, sz, err := s.FetchBlob(ctx, sr.Ref)
	require.NoError(t, err)
	defer rc.Close()
	require.Equal(t, uint64(len(data)), sz)

	sr, err = types.Hash(rc)
	rc.Close()
	require.NoError(t, err)
	require.Equal(t, expRef, sr)

	it := s.IterateBlobs(ctx)
	defer it.Close()

	require.True(t, it.Next())
	require.Equal(t, expRef, it.SizedRef())
	require.NoError(t, it.Err())

	require.False(t, it.Next())
	require.NoError(t, it.Err())
}
