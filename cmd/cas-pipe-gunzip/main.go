package main

import (
	"compress/gzip"
	"io"

	"github.com/dennwc/cas/pipeline"
)

func main() {
	pipeline.Main(func(w pipeline.BlobWriter, r io.Reader) error {
		zr, err := gzip.NewReader(r)
		if err != nil {
			return err
		}
		defer zr.Close()

		_, err = io.Copy(w, zr)
		if err != nil {
			return err
		}
		return w.Commit()
	})
}
