package httpstor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/dennwc/cas/storage"
	"github.com/dennwc/cas/types"
)

var (
	_ storage.Storage = (*Client)(nil)
)

func init() {
	storage.RegisterConfig("cas:HTTPClientConfig", &Config{})
}

type Config struct {
	URL string `json:"url"`
}

func (c *Config) References() []types.Ref {
	return nil
}

func (c *Config) OpenStorage(ctx context.Context) (storage.Storage, error) {
	_, err := url.Parse(c.URL)
	if err != nil {
		return nil, err
	}
	return NewClient(c.URL), nil
}

// NewClient creates a CAS HTTP client with a given base address.
//
// Example:
//		NewClient("https://domain.com/cas")
func NewClient(addr string) *Client {
	addr = strings.TrimSuffix(addr, "/")
	return &Client{
		base: addr,
		cli:  http.DefaultClient,
	}
}

// Client is a HTTP client for CAS.
type Client struct {
	cli  *http.Client
	base string
}

func (c *Client) Close() error { return nil }

// SetHTTPClient allows to set a custom HTTP client that will be used to send requests.
func (c *Client) SetHTTPClient(cli *http.Client) {
	c.cli = cli
}

func (c *Client) blobsURL() string {
	return c.base + "/blobs/"
}

func (c *Client) blobURL(ref types.Ref) string {
	return c.blobsURL() + ref.String()
}

func (c *Client) pinsURL() string {
	return c.blobsURL() + "/pins/"
}

func (c *Client) pinURL(name string) string {
	return c.pinsURL() + name
}

func (c *Client) StatBlob(ctx context.Context, ref types.Ref) (uint64, error) {
	if ref.Zero() {
		return 0, storage.ErrInvalidRef
	}
	req, err := http.NewRequest("HEAD", c.blobURL(ref), nil)
	if err != nil {
		return 0, err
	}
	req = req.WithContext(ctx)

	resp, err := c.cli.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return uint64(resp.ContentLength), nil
	case http.StatusNotFound:
		return 0, storage.ErrNotFound
	default:
		return 0, fmt.Errorf("unexpected status code on stat: %v", resp.Status)
	}
}

func (c *Client) FetchBlob(ctx context.Context, ref types.Ref) (io.ReadCloser, uint64, error) {
	if ref.Zero() {
		return nil, 0, storage.ErrInvalidRef
	}
	req, err := http.NewRequest("GET", c.blobURL(ref), nil)
	if err != nil {
		return nil, 0, err
	}
	req = req.WithContext(ctx)

	resp, err := c.cli.Do(req)
	if err != nil {
		return nil, 0, err
	}

	switch resp.StatusCode {
	case http.StatusOK:
		return resp.Body, uint64(resp.ContentLength), nil
	case http.StatusNotFound:
		resp.Body.Close()
		return nil, 0, storage.ErrNotFound
	default:
		resp.Body.Close()
		return nil, 0, fmt.Errorf("unexpected status code on stat: %v", resp.Status)
	}
}

func (c *Client) BeginBlob(ctx context.Context) (storage.BlobWriter, error) {
	return nil, storage.ErrReadOnly // TODO
}

func (c *Client) IterateBlobs(ctx context.Context) storage.Iterator {
	it := &blobsIterator{
		jsonIterator: jsonIterator{
			c: c, ctx: ctx, url: c.blobsURL(),
		},
	}
	it.dst = &it.cur
	return it
}

func (c *Client) SetPin(ctx context.Context, name string, ref types.Ref) error {
	return storage.ErrReadOnly // TODO
}

func (c *Client) DeletePin(ctx context.Context, name string) error {
	return storage.ErrReadOnly // TODO
}

func (c *Client) GetPin(ctx context.Context, name string) (types.Ref, error) {
	if strings.ContainsAny(name, "/?&") {
		return types.Ref{}, fmt.Errorf("invalid pin name: %q", name)
	}
	req, err := http.NewRequest("HEAD", c.pinURL(name), nil)
	if err != nil {
		return types.Ref{}, err
	}
	req = req.WithContext(ctx)

	resp, err := c.cli.Do(req)
	if err != nil {
		return types.Ref{}, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return types.ParseRef(resp.Header.Get("X-CAS-Ref"))
	case http.StatusNotFound:
		return types.Ref{}, storage.ErrNotFound
	default:
		return types.Ref{}, fmt.Errorf("unexpected status code on pin get: %v", resp.Status)
	}
}

func (c *Client) IteratePins(ctx context.Context) storage.PinIterator {
	it := &pinsIterator{
		jsonIterator: jsonIterator{
			c: c, ctx: ctx, url: c.pinsURL(),
		},
	}
	it.dst = &it.cur
	return it
}

type jsonIterator struct {
	c   *Client
	ctx context.Context
	url string

	resp *http.Response
	dec  *json.Decoder
	dst  interface{}
	err  error
}

func (it *jsonIterator) Next() bool {
	if it.err != nil {
		return false
	}
	if it.resp == nil {
		req, err := http.NewRequest("GET", it.url, nil)
		if err != nil {
			it.err = err
			return false
		}
		req = req.WithContext(it.ctx)

		resp, err := it.c.cli.Do(req)
		if err != nil {
			it.err = err
			return false
		} else if resp.StatusCode != 200 {
			resp.Body.Close()
			it.err = fmt.Errorf("unexpected status code on iterate: %v", resp.Status)
			return false
		}
		it.resp = resp
		it.dec = json.NewDecoder(it.resp.Body)
	}
	if err := it.dec.Decode(it.dst); err == io.EOF {
		return false
	} else if err != nil {
		it.err = err
		return false
	}
	return true
}

func (it *jsonIterator) Err() error {
	return it.err
}
func (it *jsonIterator) Close() error {
	if it.resp != nil {
		it.resp.Body.Close()
	}
	return nil
}

type blobsIterator struct {
	jsonIterator
	cur types.SizedRef
}

func (it *blobsIterator) SizedRef() types.SizedRef {
	return it.cur
}

type pinsIterator struct {
	jsonIterator
	cur types.Pin
}

func (it *pinsIterator) Pin() types.Pin {
	return it.cur
}
