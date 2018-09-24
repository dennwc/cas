package cas

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/dennwc/cas/schema"
	"github.com/dennwc/cas/types"
)

// StoreAddr interprets an address as either a local FS path or URL and fetches the content.
// It will create schema objects automatically.
func (s *Storage) StoreAddr(ctx context.Context, addr string, conf *StoreConfig) (types.SizedRef, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return types.SizedRef{}, err
	}
	conf = checkConfig(conf)
	if u.Scheme != "" {
		return s.StoreURLContent(ctx, addr, conf)
	}
	return s.StoreFilePath(ctx, addr, conf)
}

func (s *Storage) StoreURLContent(ctx context.Context, url string, conf *StoreConfig) (SizedRef, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return SizedRef{}, err
	}
	return s.StoreHTTPContent(ctx, req, conf)
}

func (s *Storage) StoreHTTPContent(ctx context.Context, req *http.Request, conf *StoreConfig) (SizedRef, error) {
	conf = checkConfig(conf)

	req = req.WithContext(ctx)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return SizedRef{}, fmt.Errorf("cannot fetch http content: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return SizedRef{}, fmt.Errorf("status: %v", resp.Status)
	}
	sr, err := s.StoreBlob(ctx, resp.Body, conf)
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
