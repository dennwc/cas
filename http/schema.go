package cashttp

import (
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/dennwc/cas/schema"
	"github.com/dennwc/cas/types"
)

func init() {
	schema.RegisterName(requestType, &Request{})
	schema.RegisterName("http:Response", &Response{})
	schema.RegisterName(sessionType, &Session{})
}

const (
	requestType = "http:Request"
	sessionType = "http:Session"
)

// Request represents an HTTP request.
type Request struct {
	Method  string         `json:"method"`
	URL     URL            `json:"url"`
	Header  Header         `json:"header,omitempty"`
	Body    types.SizedRef `json:"body,omitempty"`
	Trailer Header         `json:"trailer,omitempty"`
}

func (r *Request) References() []types.Ref {
	return []types.Ref{r.Body.Ref}
}

// Response represents an HTTP response.
type Response struct {
	Status  int            `json:"status"`
	Header  Header         `json:"header,omitempty"`
	Body    types.SizedRef `json:"body,omitempty"`
	Trailer Header         `json:"trailer,omitempty"`
}

func (r *Response) References() []types.Ref {
	return []types.Ref{r.Body.Ref}
}

// Session binds a response to a specific request.
type Session struct {
	Request  types.Ref `json:"request"`
	Response types.Ref `json:"response"`
}

func (r *Session) References() []types.Ref {
	return []types.Ref{r.Request, r.Response}
}

var (
	_ json.Marshaler   = URL{}
	_ json.Unmarshaler = (*URL)(nil)
)

type URL url.URL

func (u *URL) String() string {
	su := (*url.URL)(u)
	return su.String()
}
func (u URL) MarshalJSON() ([]byte, error) {
	return json.Marshal(u.String())
}
func (u *URL) UnmarshalJSON(p []byte) error {
	var s string
	if err := json.Unmarshal(p, &s); err != nil {
		return err
	}
	su, err := url.Parse(s)
	if err != nil {
		return err
	}
	nu := (*URL)(su)
	*u = *nu
	return nil
}

var (
	_ json.Marshaler   = Header{}
	_ json.Unmarshaler = (*Header)(nil)
)

// Header represents an HTTP header or trailer.
type Header http.Header

func (h Header) Get(key string) string {
	return http.Header(h).Get(key)
}

func (h Header) MarshalJSON() ([]byte, error) {
	m := make(map[string]interface{}, len(h))
	for k, arr := range h {
		var v interface{}
		if len(arr) == 1 {
			v = arr[0]
		} else if len(arr) != 0 {
			v = arr
		}
		m[k] = v
	}
	return json.Marshal(m)
}

func (h *Header) UnmarshalJSON(p []byte) error {
	hd := make(Header)
	*h = hd
	m := make(map[string]json.RawMessage)
	err := json.Unmarshal(p, &m)
	if err != nil {
		return err
	}
	for k, b := range m {
		var s string
		if err = json.Unmarshal(b, &s); err == nil {
			hd[k] = []string{s}
			continue
		}
		var arr []string
		err = json.Unmarshal(b, &arr)
		if err != nil {
			return err
		}
		hd[k] = arr
	}
	return nil
}
