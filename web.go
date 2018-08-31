package cas

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/dennwc/cas/schema"
	"github.com/dennwc/cas/types"
)

func NewWebContentRefTo(w io.Writer, url string) (*schema.WebContent, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status: %v", resp.Status)
	}
	var r io.Reader = resp.Body
	if w != nil {
		r = io.TeeReader(r, w)
	}
	ref, size, err := types.Hash(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, err
	}
	c := &schema.WebContent{
		URL: url, Ref: ref, Size: uint64(size),
		ETag: strings.Trim(resp.Header.Get("ETag"), `"`),
	}
	if v := resp.Header.Get("Last-Modified"); v != "" {
		t, err := time.Parse(time.RFC1123, v)
		if err != nil {
			return c, err
		}
		t = t.UTC()
		c.TS = &t
	} else if c.ETag == "" {
		t := time.Now().UTC()
		c.TS = &t
	}
	return c, nil
}
func NewWebContentRef(url string) (*schema.WebContent, error) {
	return NewWebContentRefTo(nil, url)
}
