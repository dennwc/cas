package config

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"

	"github.com/dennwc/cas/storage"
)

const (
	DefaultConfigExt = ".json"
	DefaultConfig    = "config" + DefaultConfigExt
)

// Config stores all configuration of a CAS.
type Config struct {
	// Storage is a config for a primary storage used in this CAS.
	Storage storage.Config
}

// ReadConfig reads a CAS config file from a given path.
// It will add an name of default config if the specified path ends with a separator.
// It will also guess a file extension if it was not specified.
func ReadConfig(path string) (*Config, error) {
	if strings.HasSuffix(path, string(filepath.Separator)) {
		path += DefaultConfig
	} else if filepath.Ext(path) == "" {
		path += DefaultConfigExt
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var c struct {
		Storage json.RawMessage `json:"storage"`
	}
	// TODO: should use TOML; but we rely on schema.Decode that only accepts JSON
	if err = json.NewDecoder(f).Decode(&c); err != nil {
		return nil, err
	}
	var conf Config
	if len(c.Storage) != 0 {
		sc, err := storage.DecodeConfig(bytes.NewReader(c.Storage))
		if err != nil {
			return nil, err
		}
		conf.Storage = sc
	}
	return &conf, nil
}

// WriteConfig writes a CAS config file to a given path.
// It will add an name of default config if the specified path ends with a separator.
// It will also guess a file extension if it was not specified.
func WriteConfig(path string, conf *Config) error {
	if strings.HasSuffix(path, string(filepath.Separator)) {
		path += DefaultConfig
	} else if filepath.Ext(path) == "" {
		path += DefaultConfigExt
	}

	buf := new(bytes.Buffer)
	err := storage.EncodeConfig(buf, conf.Storage)
	if err != nil {
		return err
	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var c struct {
		Storage json.RawMessage `json:"storage"`
	}
	c.Storage = json.RawMessage(buf.Bytes())
	enc := json.NewEncoder(f)
	// synchronized with schema.Encode
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "\t")
	err = enc.Encode(c)
	if err != nil {
		return err
	}
	return f.Close()
}
