package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/dennwc/cas"
	"github.com/dennwc/cas/storage"
	"github.com/dennwc/cas/types"
)

func init() {
	cmd := &cobra.Command{
		Use:     "pipeline cmd blob [blobs...]",
		Aliases: []string{"pipe"},
		Short:   "process blobs via a pipeline",
		RunE: casOpenCmd(func(ctx context.Context, s *cas.Storage, _ *pflag.FlagSet, args []string) error {
			if len(args) < 2 {
				return fmt.Errorf("expected at least 2 arguments")
			}
			cname := args[0]
			if !strings.ContainsAny(cname, "/.\\") {
				cname = "cas-pipe-" + cname
			}
			args = args[1:]
			for _, sref := range args {
				ref, err := types.ParseRef(sref)
				if err != nil {
					return err
				}
				sr, err := process(ctx, s, cname, ref)
				if err != nil {
					fmt.Println(ref, err)
					continue
				}
				fmt.Println(ref, "->", sr.Ref)
			}
			return nil
		}),
	}
	Root.AddCommand(cmd)
}

func process(ctx context.Context, s *cas.Storage, cname string, ref types.Ref) (types.SizedRef, error) {
	rc, _, err := s.FetchBlob(ctx, ref)
	if err != nil {
		return types.SizedRef{}, err
	}
	defer rc.Close()

	ebuf := new(bytes.Buffer)
	cmd := exec.Command(cname)
	cmd.Stdin = rc
	cmd.Stderr = ebuf

	out, err := cmd.StdoutPipe()
	if err != nil {
		return types.SizedRef{}, err
	}
	defer out.Close()

	if err := cmd.Start(); err != nil {
		return types.SizedRef{}, err
	}
	defer cmd.Wait()

	buf := make([]byte, 32*1024)
	var w storage.BlobWriter
	for {
		n, err := out.Read(buf)
		if err == io.EOF && n != 0 {
			err = nil
		}
		if err == io.EOF {
			break
		} else if err != nil {
			return types.SizedRef{}, err
		}
		buf = buf[:n]
		if w == nil {
			w, err = s.BeginBlob(ctx)
			if err != nil {
				return types.SizedRef{}, err
			}
			defer w.Close()
		}
		_, err = w.Write(buf)
		if err != nil {
			return types.SizedRef{}, err
		}
	}
	err = cmd.Wait()
	if err != nil {
		errb := bytes.TrimSpace(ebuf.Bytes())
		if len(errb) != 0 {
			return types.SizedRef{}, errors.New(string(errb))
		}
		return types.SizedRef{}, err
	} else if w == nil {
		return types.SizedRef{}, errors.New("empty output discarded")
	}
	sr, err := w.Complete()
	if err != nil {
		return types.SizedRef{}, err
	}
	err = w.Commit()
	if err != nil {
		return types.SizedRef{}, err
	}
	return sr, nil
}
