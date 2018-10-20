package filter_test

import (
	"testing"

	"github.com/dennwc/cas/schema/filter"
	"github.com/stretchr/testify/require"
)

var casesFilter = []struct {
	name   string
	obj    interface{}
	filter string
	exp    bool
}{
	{
		name: "json tag",
		obj: struct {
			Status int `json:"status"`
		}{Status: 403},
		filter: "s.status == 403",
		exp:    true,
	},
}

func TestFilter(t *testing.T) {
	for _, c := range casesFilter {
		t.Run(c.name, func(t *testing.T) {
			f, err := filter.Compile(c.filter)
			require.NoError(t, err)
			v, err := f.FilterObject(c.obj)
			require.NoError(t, err)
			require.Equal(t, c.exp, v)
		})
	}
}
