package local

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dennwc/cas/storage"
	"github.com/dennwc/cas/storage/test"
)

func TestLocalDir(t *testing.T) {
	storagetest.RunTests(t, func(t testing.TB) (storage.Storage, func()) {
		dir, err := os.MkdirTemp("", "cas_local_")
		require.NoError(t, err)
		cleanup := func() {
			os.RemoveAll(dir)
		}
		s, err := New(dir, true)
		if err != nil {
			cleanup()
		}
		require.NoError(t, err)
		return s, cleanup
	})
}
