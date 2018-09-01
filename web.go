package cas

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/dennwc/cas/schema"
	"github.com/dennwc/cas/types"
)

func (s *Storage) storeHTTPContent(ctx context.Context, req *http.Request, index bool) (SizedRef, error) {
	req = req.WithContext(ctx)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return SizedRef{}, fmt.Errorf("cannot fetch http content: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return SizedRef{}, fmt.Errorf("status: %v", resp.Status)
	}
	var sr SizedRef
	if index {
		sr, err = types.Hash(resp.Body)
	} else {
		sr, err = s.StoreBlob(ctx, types.Ref{}, resp.Body)
	}
	if err != nil {
		return SizedRef{}, err
	}
	resp.Body.Close()

	m := schema.WebContent{
		URL: req.URL.String(), Ref: sr.Ref, Size: sr.Size,
		ETag: strings.Trim(resp.Header.Get("ETag"), `"`),
	}
	if v := resp.Header.Get("Last-Modified"); v != "" {
		if t, err := time.Parse(time.RFC1123, v); err == nil {
			t = t.UTC()
			m.TS = &t
		}
	} else if m.ETag == "" {
		t := time.Now().UTC()
		m.TS = &t
	}
	return s.StoreSchema(ctx, &m)
}

func (s *Storage) storeURLContent(ctx context.Context, url string, index bool) (SizedRef, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return SizedRef{}, err
	}
	return s.storeHTTPContent(ctx, req, index)
}

func (s *Storage) StoreURLContent(ctx context.Context, url string) (SizedRef, error) {
	return s.storeURLContent(ctx, url, false)
}

func (s *Storage) IndexURLContent(ctx context.Context, url string) (SizedRef, error) {
	return s.storeURLContent(ctx, url, true)
}

func (s *Storage) StoreHTTPContent(ctx context.Context, req *http.Request) (SizedRef, error) {
	return s.storeHTTPContent(ctx, req, false)
}
func (s *Storage) IndexHTTPContent(ctx context.Context, req *http.Request) (SizedRef, error) {
	return s.storeHTTPContent(ctx, req, true)
}
