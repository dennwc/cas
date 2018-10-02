package httpstor

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/dennwc/cas/storage"
	"github.com/dennwc/cas/types"
)

// NewServer creates a CAS HTTP server for a given URL path.
func NewServer(s storage.Storage, urlPref string) http.Handler {
	urlPref = strings.TrimSuffix(urlPref, "/")
	return &server{s: s, pref: urlPref}
}

type server struct {
	s    storage.Storage
	pref string
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" && r.Method != "HEAD" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, s.pref)
	path = strings.Trim(path, "/")
	sub := strings.SplitN(path, "/", 3)

	kind := sub[0]
	sub = sub[1:]
	switch kind {
	case "blobs":
		if len(sub) == 0 {
			s.serveBlobsList(w, r)
			return
		} else if len(sub) != 1 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		ref, err := types.ParseRef(sub[0])
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(err.Error()))
			return
		}
		s.serveBlob(w, r, ref)
		return
	case "pins":
		if len(sub) == 0 {
			s.servePinsList(w, r)
			return
		} else if len(sub) != 1 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		s.servePin(w, r, sub[0])
		return
	}
	w.WriteHeader(http.StatusForbidden)
}

func (s *server) serveIter(w http.ResponseWriter, r *http.Request, it storage.BaseIterator, item func(storage.BaseIterator) interface{}) {
	defer it.Close()
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	for it.Next() {
		err := enc.Encode(item(it))
		if err != nil {
			return // write error, client is probably gone; ok to ignore
		}
	}
	// TODO: status code was already sent, so we can't report it; consider sending it in body
	err := it.Err()
	if err != nil {
		log.Println("http: error when iterating blobs:", err)
	}
}

func (s *server) serveBlobsList(w http.ResponseWriter, r *http.Request) {
	it := s.s.IterateBlobs(r.Context())
	defer it.Close()
	s.serveIter(w, r, it, func(it storage.BaseIterator) interface{} {
		return it.(storage.Iterator).SizedRef()
	})
}

func (s *server) serveBlob(w http.ResponseWriter, r *http.Request, ref types.Ref) {
	if r.Method != "GET" && r.Method != "HEAD" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	// TODO: handle If-None-Match and write ETag for non-CAS clients
	switch r.Method {
	case "HEAD":
		sz, err := s.s.StatBlob(r.Context(), ref)
		if err == storage.ErrNotFound {
			w.WriteHeader(http.StatusNotFound)
			return
		} else if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		w.Header().Set("Content-Length", strconv.FormatUint(sz, 10))
		w.Header().Set("X-CAS-Ref", ref.String())
		return
	case "GET":
		rc, sz, err := s.s.FetchBlob(r.Context(), ref)
		if err == storage.ErrNotFound {
			w.WriteHeader(http.StatusNotFound)
			return
		} else if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		defer rc.Close()
		w.Header().Set("Content-Length", strconv.FormatUint(sz, 10))
		w.Header().Set("X-CAS-Ref", ref.String())
		_, _ = io.Copy(w, rc)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (s *server) servePinsList(w http.ResponseWriter, r *http.Request) {
	it := s.s.IteratePins(r.Context())
	defer it.Close()
	s.serveIter(w, r, it, func(it storage.BaseIterator) interface{} {
		return it.(storage.PinIterator).Pin()
	})
}

func (s *server) servePin(w http.ResponseWriter, r *http.Request, name string) {
	if r.Method != "GET" && r.Method != "HEAD" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	ref, err := s.s.GetPin(r.Context(), name)
	if err == storage.ErrNotFound {
		w.WriteHeader(http.StatusNotFound)
		return
	} else if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.Header().Set("X-CAS-Ref", ref.String())
	// TODO: handle If-None-Match and write ETag for non-CAS clients
	switch r.Method {
	case "HEAD":
		return
	case "GET":
		// shorthand for regular HTTP clients - redirect to a blob URL
		http.Redirect(w, r, s.pref+"/blobs/"+ref.String(), http.StatusFound)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}
