// Package cashttp implement HTTP cache based on CAS.
package cashttp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/dennwc/cas"
	"github.com/dennwc/cas/schema"
	"github.com/dennwc/cas/storage"
	"github.com/dennwc/cas/types"
)

// NewTransport creates an HTTP transport based on CAS that will store all the requests and responses
// and will replay them if request is sent again.
//
// Requests will be matched against the cache based on exact URL match and on match of well-known headers (like Accept).
// See MatchHeaders for more details.
func NewTransport(s *cas.Storage) *Transport {
	t := &Transport{s: s, tr: http.DefaultTransport}
	t.MatchHeaders(
		"Accept",
		"If-Modified-Since",
		"If-None-Match",
	)
	return t
}

var _ http.RoundTripper = (*Transport)(nil)

type Transport struct {
	s  *cas.Storage
	tr http.RoundTripper

	matchHeader []string
	reqFilter   func(*http.Request) bool
	respFilter  func(*http.Response) bool
}

// MatchHeaders adds additional headers that will be used to match requests versus cache entries.
func (t *Transport) MatchHeaders(list ...string) {
	t.matchHeader = append(t.matchHeader, list...)
}

// SetTransport sets an underlying transport that is used to fill the cache.
func (t *Transport) SetTransport(tr http.RoundTripper) {
	t.tr = tr
}

// RequestFilter allows to ignore some requests so they are not stored in CAS.
// Caller should not inspect the body, or should replace it with a new copy.
func (t *Transport) RequestFilter(fnc func(*http.Request) bool) {
	t.reqFilter = fnc
}

// ResponseFilter allows to ignore some responses so they are not stored in CAS.
// Caller should not inspect the body, or should replace it with a new copy.
func (t *Transport) ResponseFilter(fnc func(*http.Response) bool) {
	t.respFilter = fnc
}

// RoundTrip implements http.RoundTripper.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.reqFilter != nil && !t.reqFilter(req) {
		return t.tr.RoundTrip(req)
	}
	ctx := req.Context()

	// read the body first - we need to store the trailer, so we need to drain the stream
	body, bref, err := t.storeBody(ctx, req.Body)
	if err != nil {
		req.Body.Close()
		return nil, err
	}
	req.Body = body // restore the body

	// do not store request metadata just yet
	// user may provide a custom headers filter, so we might have a similar
	// request already stored in the cache

	if resp, err := t.serveFromCache(req); err == nil && resp != nil {
		// no need to store the new request
		return resp, nil
	} else if err != nil {
		log.Println(req.Method, req.URL, err)
	}

	// cache miss - store the request
	reqRef, err := t.storeRequest(req, bref)
	if err != nil {
		req.Body.Close()
		return nil, err
	}

	resp, err := t.tr.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	if t.respFilter != nil && !t.respFilter(resp) {
		return resp, nil
	}
	respRef, err := t.storeResponse(req.Context(), resp)
	if err != nil {
		resp.Body.Close()
		return nil, err
	}
	sessRef, err := t.storeSession(ctx, reqRef, respRef)
	if err != nil {
		resp.Body.Close()
		return nil, err
	}
	resp.Header.Set("X-CAS-Session-Ref", sessRef.String())
	return resp, nil
}

func (t *Transport) storeBody(ctx context.Context, rc io.ReadCloser) (io.ReadCloser, types.SizedRef, error) {
	if rc == nil {
		return nil, types.SizedRef{Ref: types.BytesRef(nil)}, nil
	}
	defer rc.Close()
	// read the small buffer to know if the stream is empty
	const peekSize = 4096
	b := make([]byte, peekSize)
	n, err := rc.Read(b[:])
	if err == io.EOF {
		err = nil
		if n == 0 {
			// don't bother saving it, Storage can generate it on the flight
			ref := types.BytesRef(nil)
			sr := types.SizedRef{Ref: ref}
			return ioutil.NopCloser(bytes.NewReader(nil)), sr, nil
		}
	}
	if err != nil {
		return nil, types.SizedRef{}, err
	}
	b = b[:n]
	if len(b) < peekSize {
		// TODO: bake this into CAS itself

		// check if its already a EOF - in this case we can calculate the hash
		// and check if blob is already in the store

		n, err = rc.Read(b[len(b):peekSize])
		b = b[:len(b)+n]
		if err == io.EOF {
			ref := types.BytesRef(b)
			if sz, err := t.s.StatBlob(ctx, ref); err == nil {
				// blob is in the store - return
				return ioutil.NopCloser(bytes.NewReader(b)), types.SizedRef{Ref: ref, Size: sz}, nil
			}
			// not in the store - continue as usual
		} else if err != nil {
			return nil, types.SizedRef{}, err
		}
	}

	// stream is not empty or does not exist - store it
	w, err := t.s.BeginBlob(ctx)
	if err != nil {
		return nil, types.SizedRef{}, err
	}
	defer w.Close()

	// don't forget to join our buffer
	_, err = io.Copy(w, io.MultiReader(bytes.NewReader(b), rc))
	if err != nil {
		return nil, types.SizedRef{}, err
	}
	sr, err := w.Complete()
	if err != nil {
		return nil, types.SizedRef{}, err
	}
	err = w.Commit()
	if err != nil {
		return nil, types.SizedRef{}, err
	}

	// blob stored, but we need to replay it now
	nrc, _, err := t.s.FetchBlob(ctx, sr.Ref)
	return nrc, sr, err
}

func (t *Transport) storeRequest(req *http.Request, bref types.SizedRef) (types.Ref, error) {
	ctx := req.Context()
	// store request schema
	r := &Request{
		Method:  req.Method,
		URL:     *((*URL)(req.URL)),
		Header:  Header(req.Header),
		Body:    bref,
		Trailer: Header(req.Trailer),
	}
	sr, err := t.s.StoreSchema(ctx, r)
	if err != nil {
		return types.Ref{}, err
	}
	return sr.Ref, nil
}

func (t *Transport) storeResponse(ctx context.Context, resp *http.Response) (types.Ref, error) {
	// read the body first, because we need to check the trailer
	body, bref, err := t.storeBody(ctx, resp.Body)
	if err != nil {
		return types.Ref{}, err
	}
	resp.Body = body // restore the body

	// store response schema
	r := &Response{
		Status:  resp.StatusCode,
		Header:  Header(resp.Header),
		Body:    bref,
		Trailer: Header(resp.Trailer),
	}
	sr, err := t.s.StoreSchema(ctx, r)
	if err != nil {
		return types.Ref{}, err
	}
	return sr.Ref, nil
}

func (t *Transport) storeSession(ctx context.Context, req, resp types.Ref) (types.Ref, error) {
	// store session schema
	sr, err := t.s.StoreSchema(ctx, &Session{
		Request:  req,
		Response: resp,
	})
	if err != nil {
		return types.Ref{}, err
	}
	return sr.Ref, nil
}

func (t *Transport) requestMatches(req *http.Request, obj *Request) bool {
	if req.Method != obj.Method {
		return false
	}
	u1, u2 := req.URL, obj.URL
	if u1.Scheme != u2.Scheme || u1.Host != u2.Host || u1.Path != u2.Path {
		return false
	} else if u1.String() != u2.String() {
		return false
	}
	for _, h := range t.matchHeader {
		arr1, ok1 := req.Header[h]
		arr2, ok2 := obj.Header[h]
		if ok1 != ok2 || len(arr1) != len(arr2) {
			return false
		}
		for i := range arr1 {
			if arr1[i] != arr2[i] {
				return false
			}
		}
	}
	// TODO: cache expiration?
	return true
}

// checkReqCache checks if this request is cached and returns a ref of the response, if any.
func (t *Transport) checkReqCache(req *http.Request) (*Response, error) {
	// in fact, we cannot use request ref because it might contain additional headers
	// instead, we will check all request object and match them according to our rules
	ctx := req.Context()
	it := t.s.IterateSchema(ctx, requestType)
	defer it.Close()

	var last error
	for it.Next() {
		obj, err := it.Decode()
		if err != nil {
			return nil, err
		}
		r, ok := obj.(*Request)
		if !ok {
			return nil, fmt.Errorf("unexpected type: %T", obj)
		}
		if t.requestMatches(req, r) {
			// it's not enough to match it, it should also have a response
			respRef, err := t.findResponseFor(ctx, it.SizedRef().Ref)
			if err != nil {
				last = err
				continue
			} else if respRef.Zero() {
				continue
			}
			// and response should exist
			obj, err := t.s.DecodeSchema(ctx, respRef)
			if err == schema.ErrNotSchema || err == storage.ErrNotFound {
				continue
			} else if err != nil {
				return nil, fmt.Errorf("failed to decode response: %v", err)
			}
			resp, ok := obj.(*Response)
			if !ok {
				continue
			}
			return resp, nil
		}
	}
	return nil, last
}

func (t *Transport) findResponseFor(ctx context.Context, req types.Ref) (types.Ref, error) {
	it := t.s.IterateSchema(ctx, sessionType)
	defer it.Close()
	for it.Next() {
		obj, err := it.Decode()
		if err != nil {
			return types.Ref{}, err
		}
		r, ok := obj.(*Session)
		if !ok {
			return types.Ref{}, fmt.Errorf("unexpected type: %T", obj)
		}
		if r.Request == req {
			return r.Response, nil
		}
	}
	return types.Ref{}, nil
}

func (t *Transport) reconstruct(ctx context.Context, r *Response) (*http.Response, error) {
	resp := &http.Response{
		StatusCode:    r.Status,
		Status:        fmt.Sprintf("%d %s", r.Status, http.StatusText(r.Status)),
		Header:        http.Header(r.Header),
		Trailer:       http.Header(r.Trailer),
		ContentLength: int64(r.Body.Size),
	}
	if r.Body.Ref.Zero() || r.Body.Ref.Empty() {
		resp.Body = ioutil.NopCloser(bytes.NewReader(nil))
	} else {
		rc, _, err := t.s.FetchBlob(ctx, r.Body.Ref)
		if err != nil {
			return nil, err
		}
		resp.Body = rc
	}
	return resp, nil
}

func (t *Transport) serveFromCache(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	resp, err := t.checkReqCache(req)
	if err != nil || resp == nil {
		return nil, err
	}
	return t.reconstruct(ctx, resp)
}
