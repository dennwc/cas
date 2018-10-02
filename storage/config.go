package storage

import (
	"context"
	"fmt"
	"io"

	"github.com/dennwc/cas/schema"
)

// RegisterConfig registers a new storage config.
func RegisterConfig(typ string, o Config) {
	schema.RegisterName(typ, o)
}

// Config is an interface for storage configuration objects.
type Config interface {
	schema.Object
	// OpenStorage uses the config to make a new storage implementation.
	OpenStorage(ctx context.Context) (Storage, error)
}

// DecodeConfig reads and decodes config from a specified stream.
func DecodeConfig(r io.Reader) (Config, error) {
	obj, err := schema.DecodeJSON(r)
	if err != nil {
		return nil, err
	}
	conf, ok := obj.(Config)
	if !ok {
		return nil, fmt.Errorf("%T is not a storage config", obj)
	}
	return conf, nil
}

// EncodeConfig encodes a storage config to a specified writer.
func EncodeConfig(w io.Writer, o Config) error {
	return schema.Encode(w, o)
}
