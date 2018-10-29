package types

import (
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRefZero(t *testing.T) {
	var r Ref
	require.True(t, r.Zero())
}

func TestRefEmpty(t *testing.T) {
	r := StringRef("")
	require.True(t, r.Empty())
	require.True(t, !r.Zero())
}

func TestRef(t *testing.T) {
	b := sha256.Sum256([]byte("abc"))

	const s = `sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad`

	r, err := ParseRef(s)
	require.NoError(t, err)
	require.Equal(t, "sha256", r.name)
	require.Equal(t, b, r.data)
	require.Equal(t, s, r.String())
}
