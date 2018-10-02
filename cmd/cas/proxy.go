package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/dennwc/cas"
	"github.com/dennwc/cas/schema"
	"github.com/dennwc/cas/storage"
)

var typeWebContent = schema.MustTypeOf(&schema.WebContent{})

func init() {

	cmd := &cobra.Command{
		Use:   "proxy",
		Short: "proxy http requests and save them to CAS",
		RunE: casOpenCmd(func(ctx context.Context, s *cas.Storage, flags *pflag.FlagSet, args []string) error {
			if len(args) != 0 {
				return fmt.Errorf("unexpected argument")
			}
			host, _ := flags.GetString("host")

			log.Println("listening on", host)
			return http.ListenAndServe(host, &proxy{s: s})
		}),
	}
	cmd.Flags().String("host", "localhost:9080", "host to listen on")
	Root.AddCommand(cmd)
}

type proxy struct {
	s *cas.Storage
}

func (p *proxy) cleanRequest(r *http.Request) {
	// If no Accept-Encoding header exists, Transport will add the headers it can accept
	// and would wrap the response body with the relevant reader.
	r.Header.Del("Accept-Encoding")
	// curl can add that, see
	// https://jdebp.eu./FGA/web-proxy-connection-header.html
	r.Header.Del("Proxy-Connection")
	r.Header.Del("Proxy-Authenticate")
	r.Header.Del("Proxy-Authorization")
	// Connection, Authenticate and Authorization are single hop Header:
	// http://www.w3.org/Protocols/rfc2616/rfc2616.txt
	// 14.10 Connection
	//   The Connection general-header field allows the sender to specify
	//   options that are desired for that particular connection and MUST NOT
	//   be communicated by proxies over further connections.
	r.Header.Del("Connection")
}

func (p *proxy) serveObject(r *http.Request, c *schema.WebContent) *http.Response {
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
	}
	resp.ContentLength = int64(c.Size)
	resp.Header.Set("Content-Length", strconv.FormatUint(c.Size, 10))
	resp.Header.Set("Server", "CAS")
	if c.ETag != "" {
		resp.Header.Set("ETag", `"`+c.ETag+`"`)
	}
	if c.TS != nil {
		t := *c.TS
		resp.Header.Set("Last-Modified", t.UTC().Format(time.RFC1123))
	}
	rc, _, err := p.s.FetchBlob(r.Context(), c.Ref)
	if err != nil {
		log.Println("cannot fetch blob:", err)
		return nil
	}
	buf := make([]byte, 512)
	n, err := rc.Read(buf)
	if err == io.EOF {
		err = nil
	} else if err != nil {
		rc.Close()
		log.Println("cannot fetch blob:", err)
		return nil
	}
	buf = buf[:n]
	resp.Header.Set("Content-Type", http.DetectContentType(buf))
	if r.Method == "HEAD" {
		rc.Close()
		resp.Body = ioutil.NopCloser(strings.NewReader(""))
		return resp
	}
	resp.Body = struct {
		io.Reader
		io.Closer
	}{
		Reader: io.MultiReader(bytes.NewReader(buf), rc),
		Closer: rc,
	}
	return resp
}

func (p *proxy) checkCache(r *http.Request) *http.Response {
	if r.Method != "GET" && r.Method != "HEAD" {
		return nil
	}
	it := p.s.IterateSchema(r.Context(), typeWebContent)
	defer it.Close()

	surl := r.URL.String()
	for it.Next() {
		obj, err := it.Decode()
		if err != nil {
			log.Println("error:", err)
			return nil
		}
		c, ok := obj.(*schema.WebContent)
		if !ok {
			log.Printf("unexpected object: %T", obj)
			return nil
		}
		if c.URL == surl {
			return p.serveObject(r, c)
		}
	}
	return nil
}

type blobCacher struct {
	ctx context.Context
	s   *cas.Storage
	rc  io.ReadCloser
	bw  storage.BlobWriter
	obj *schema.WebContent
}

func (rc *blobCacher) write(p []byte) {
	if rc.bw == nil {
		return
	}
	_, err := rc.bw.Write(p)
	if err != nil {
		rc.bw.Close()
		rc.bw = nil
		return
	}
}

func (rc *blobCacher) complete() {
	if rc.bw == nil {
		return
	}
	sr, err := rc.bw.Complete()
	if err != nil {
		log.Println("error:", err)
		rc.bw.Close()
		rc.bw = nil
		return
	}
	rc.obj.Ref, rc.obj.Size = sr.Ref, sr.Size
	if _, err = rc.s.StatBlob(rc.ctx, sr.Ref); err == nil {
		// don't store the blob again
		rc.bw.Close()
		rc.bw = nil
	} else {
		err = rc.bw.Commit()
		rc.bw = nil
		if err != nil {
			log.Println("error:", err)
			return
		}
	}
	sr, err = rc.s.StoreSchema(rc.ctx, rc.obj)
	if err != nil {
		log.Println("cannot store schema:", err)
		return
	}
	log.Println("cached:", rc.obj.URL, sr.Ref, sr.Size)
}

func (rc *blobCacher) discard() {
	if rc.bw == nil {
		return
	}
	rc.bw.Close()
}

func (rc *blobCacher) Read(p []byte) (int, error) {
	n, err := rc.rc.Read(p)
	if n != 0 {
		rc.write(p[:n])
	}
	if err == io.EOF {
		rc.complete()
	} else if err != nil {
		rc.discard()
	}
	return n, err
}

func (rc *blobCacher) Close() error {
	rc.discard()
	return rc.rc.Close()
}

func (p *proxy) cacheResponse(req *http.Request, resp *http.Response) {
	if req.Method != "GET" {
		return
	}
	bw, err := p.s.BeginBlob(req.Context())
	if err != nil {
		log.Println("cannot write blob:", err)
		return
	}
	resp.Body = &blobCacher{
		s: p.s, ctx: req.Context(),
		rc: resp.Body, bw: bw,
		obj: cas.NewWebContent(req, resp),
	}
}

func (p *proxy) roundtrip(r *http.Request) (*http.Response, error) {
	log.Printf("< %v %v %v", r.Method, r.URL, r.Header)
	if resp := p.checkCache(r); resp != nil {
		log.Printf("> %v %v (cached)", r.Method, r.URL)
		return resp, nil
	}
	resp, err := http.DefaultTransport.RoundTrip(r)
	log.Printf("> %v %v", r.Method, r.URL)
	if err == nil {
		p.cacheResponse(r, resp)
	}
	return resp, err
}

func (p *proxy) proxyResponse(w http.ResponseWriter, resp *http.Response) {
	h := w.Header()
	for k, v := range resp.Header {
		h[k] = v
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func (p *proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == "CONNECT" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write([]byte("HTTPS is not supported"))
		return
	}
	p.cleanRequest(r)

	resp, err := p.roundtrip(r)
	if err != nil {
		w.WriteHeader(http.StatusBadGateway)
		w.Write([]byte(err.Error()))
		return
	}
	defer resp.Body.Close()

	p.proxyResponse(w, resp)
}
