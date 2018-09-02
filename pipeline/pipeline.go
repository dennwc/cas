package pipeline

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
)

var (
	errDiscard = errors.New("blob discarded")
	errDone    = errors.New("blob committed")
)

type BlobWriter interface {
	io.Writer
	// Commit stores the blob and closes it automatically.
	// Failing to call Commit will discard the blob.
	Commit() error
}

type TransformFunc func(w BlobWriter, r io.Reader) error

func Main(fnc TransformFunc) {
	flag.Parse()
	if err := run(fnc); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(fnc TransformFunc) error {
	w := &writer{w: os.Stdout}
	err := fnc(w, os.Stdin)
	if err == errDone {
		err = nil
	}
	if err != nil {
		return err
	} else if w.w != nil {
		return errDiscard
	}
	return nil
}

type writer struct {
	w   io.Writer
	err error
}

func (w *writer) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	} else if w.w == nil {
		return 0, errDone
	}
	n, err := w.w.Write(p)
	if err != nil {
		w.err = err
	}
	return n, err
}

func (w *writer) Commit() error {
	if w.err != nil {
		return w.err
	}
	w.w = nil
	return nil
}
