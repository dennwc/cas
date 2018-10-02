package httpstor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dennwc/cas/types"
	"github.com/stretchr/testify/require"

	"github.com/dennwc/cas/storage"
)

func TestHTTP(t *testing.T) {
	t.Run("no pref", func(t *testing.T) {
		testHTTP(t, "")
	})
	t.Run("pref", func(t *testing.T) {
		testHTTP(t, "/some/prefix")
	})
}

func testHTTP(t *testing.T, pref string) {
	ctx := context.Background()
	mem := storage.NewInMemory()

	data := []byte("some data")
	sr, err := storage.WriteBytes(ctx, mem, data)
	require.NoError(t, err)

	srv := NewServer(mem, pref)

	var h http.Handler = srv
	if pref != "" {
		mux := http.NewServeMux()
		mux.Handle(pref+"/", h)
		h = mux
	}

	hs := httptest.NewServer(h)
	defer hs.Close()

	cli := NewClient(hs.URL + pref)
	cli.SetHTTPClient(hs.Client())

	sz, err := cli.StatBlob(ctx, sr.Ref)
	require.NoError(t, err)
	require.Equal(t, uint64(len(data)), sz)

	rc, sz, err := cli.FetchBlob(ctx, sr.Ref)
	require.NoError(t, err)
	defer rc.Close()
	require.Equal(t, uint64(len(data)), sz)

	sr2, err := types.Hash(rc)
	rc.Close()
	require.NoError(t, err)
	require.Equal(t, sr, sr2)

	it := cli.IterateBlobs(ctx)
	defer it.Close()

	require.True(t, it.Next())
	require.Equal(t, sr, it.SizedRef())
	require.NoError(t, it.Err())

	require.False(t, it.Next())
	require.NoError(t, it.Err())
}
