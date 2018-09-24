package schema

import (
	"bytes"
	"testing"

	"github.com/dennwc/cas/types"
	"github.com/stretchr/testify/require"
)

func TestEncode(t *testing.T) {
	var tests = []struct {
		name string
		obj  Object
		exp  string
	}{
		{
			name: "file",
			obj: &DirEntry{
				Ref: types.StringRef("abc"),
				Stats: Stats{
					StatDataSize: 127,
				},
				Name: "file.dat",
			},
			exp: `{
 "@type": "cas:DirEntry",
 "ref": "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
 "name": "file.dat",
 "stats": {
  "size": 127
 }
}
`,
		},
	}
	for _, c := range tests {
		c := c
		t.Run(c.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			err := Encode(buf, c.obj)
			require.NoError(t, err)
			require.Equal(t, c.exp, buf.String())
		})
	}
}
