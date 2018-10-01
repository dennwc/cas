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
	reqRef, err := t.storeRequest(req)
	if err != nil {
		req.Body.Close()
		return nil, err
	}
	if resp, err := t.serveFromCache(reqRef, req); err == nil && resp != nil {
		return resp, nil
	} else if err != nil {
		log.Println(req.Method, req.URL, err)
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
	b := make([]byte, 512)
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

	// stream is not empty - store it
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

func (t *Transport) storeRequest(req *http.Request) (types.Ref, error) {
	ctx := req.Context()
	// read the body first, because we need to check the trailer
	body, bref, err := t.storeBody(ctx, req.Body)
	if err != nil {
		return types.Ref{}, err
	}
	req.Body = body // restore the body

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
func (t *Transport) checkReqCache(_ types.Ref, req *http.Request) (types.Ref, error) {
	// in fact, we cannot use request ref because it might contain additional headers
	// instead, we will check all request object and match them according to our rules
	ctx := req.Context()
	it := t.s.IterateSchema(ctx, requestType)
	defer it.Close()

	var last error
	for it.Next() {
		obj, err := it.Decode()
		if err != nil {
			return types.Ref{}, err
		}
		r, ok := obj.(*Request)
		if !ok {
			return types.Ref{}, fmt.Errorf("unexpected type: %T", obj)
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
			return respRef, nil
		}
	}
	return types.Ref{}, last
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

func (t *Transport) serveFromCache(ref types.Ref, req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	respRef, err := t.checkReqCache(ref, req)
	if err != nil || respRef.Zero() {
		return nil, err
	}
	obj, err := t.s.DecodeSchema(ctx, respRef)
	if err != nil {
		return nil, err
	}
	r, ok := obj.(*Response)
	if !ok {
		return nil, fmt.Errorf("unexpected type: %T", obj)
	}
	return t.reconstruct(ctx, r)
}
