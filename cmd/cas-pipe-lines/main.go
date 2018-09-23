package main

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/dennwc/cas/pipeline"
)

func main() {
	pipeline.Main(func(w pipeline.BlobWriter, r io.Reader) error {
		buf := make([]byte, 32*1024)
		lines := 0
		for {
			n, err := r.Read(buf)
			// TODO: a less naive approach
			lines += bytes.Count(buf[:n], []byte("\n"))
			if err == io.EOF {
				break
			} else if err != nil {
				return err
			}
		}
		err := json.NewEncoder(w).Encode(map[string]int{
			"lines": lines,
		})
		if err != nil {
			return err
		}
		return w.Commit()
	})
}
