package cas

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/dennwc/cas/schema"
	"github.com/dennwc/cas/storage"
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

func NewWebContent(req *http.Request, resp *http.Response) *schema.WebContent {
	m := schema.WebContent{
		URL:  req.URL.String(),
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
	return &m
}

func (s *Storage) storeWebContentSchema(ctx context.Context, sr SizedRef, req *http.Request, resp *http.Response) (SizedRef, error) {
	if sr.Ref.Zero() {
		return types.SizedRef{}, errors.New("empty ref received")
	}
	m := NewWebContent(req, resp)
	m.Ref, m.Size = sr.Ref, sr.Size
	return s.StoreSchema(ctx, m)
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

	return s.storeWebContentSchema(ctx, sr, req, resp)
}

func (s *Storage) SyncBlob(ctx context.Context, ref Ref) (Ref, error) {
	obj, err := s.DecodeSchema(ctx, ref)
	if err == schema.ErrNotSchema {
		return ref, nil
	} else if err != nil {
		return Ref{}, err
	}
	switch obj := obj.(type) {
	case *schema.WebContent:
		return s.syncWebContent(ctx, ref, obj)
	default:
		return ref, nil
	}
}

func (s *Storage) syncWebContent(ctx context.Context, oref Ref, obj *schema.WebContent) (Ref, error) {
	req, err := http.NewRequest("GET", obj.URL, nil)
	if err != nil {
		return Ref{}, err
	}
	if obj.ETag != "" {
		req.Header.Set("If-None-Match", `"`+obj.ETag+`"`)
	}
	if obj.TS != nil {
		t := *obj.TS
		// TODO: this will print "UTC" instead of "GMT"
		req.Header.Set("If-Modified-Since", t.UTC().Format(time.RFC1123))
	}
	req.Header.Set("X-CAS-If-None-Match", obj.Ref.String())

	req = req.WithContext(ctx)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return Ref{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotModified || resp.StatusCode == http.StatusPreconditionFailed {
		return oref, nil // up-to-date
	} else if resp.StatusCode != http.StatusOK {
		return Ref{}, fmt.Errorf("status: %v", resp.Status)
	}
	if resp.Header.Get("ETag") == `"`+obj.ETag+`"` {
		return oref, nil // up-to-date
	}

	// TODO: store split config somewhere?
	conf := &StoreConfig{}
	if _, err := s.StatBlob(ctx, obj.Ref); err == storage.ErrNotFound {
		// blob was stored in index-only mode
		conf.IndexOnly = true
	}

	sr, err := s.StoreBlob(ctx, resp.Body, nil)
	if err != nil {
		return Ref{}, err
	}
	resp.Body.Close()
	if sr.Ref == obj.Ref {
		return oref, nil
	}

	ssr, err := s.storeWebContentSchema(ctx, sr, req, resp)
	if err != nil {
		return sr.Ref, err
	}
	return ssr.Ref, nil
}
