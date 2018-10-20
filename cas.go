package cas

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/dennwc/cas/config"
	"github.com/dennwc/cas/storage"
	"github.com/dennwc/cas/storage/local"
	"github.com/dennwc/cas/types"
)

const (
	DefaultDir = ".cas"
	DefaultPin = "root"
)

// Init configures a CAS and stores the metadata in a specified directory.
// If directory path is empty, default path will be used.
// Relative paths in a local storage configs will be interpreted relative to the config.
func Init(dir string, conf *config.Config) error {
	if dir == "" {
		dir = DefaultDir
	}
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return err
	}
	confPath := filepath.Join(dir, config.DefaultConfig)
	_, err = config.ReadConfig(confPath)
	if err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	if conf == nil {
		conf = &config.Config{}
	}
	if conf.Storage == nil {
		// default config - store data in the same dir
		conf.Storage = &local.Config{
			Dir: ".", // same dir as config
		}
	}
	err = config.WriteConfig(confPath, conf)
	if err != nil {
		return err
	}
	// if a storage is local, initialize it as well
	c, ok := conf.Storage.(*local.Config)
	if !ok {
		return nil
	}
	// paths are relative to the config
	path := c.Dir
	if path == "" || !filepath.IsAbs(path) {
		path = filepath.Join(dir, path)
	}
	s, err := local.New(path, true)
	if err != nil {
		return err
	}
	_ = s // nothing to close
	return nil
}

type OpenOptions struct {
	Dir     string
	Storage storage.Storage
}

func Open(opt OpenOptions) (*Storage, error) {
	if opt.Storage != nil {
		return New(opt.Storage)
	}
	if opt.Dir == "" {
		opt.Dir = DefaultDir
	}
	confPath := filepath.Join(opt.Dir, config.DefaultConfig)
	conf, err := config.ReadConfig(confPath)
	if os.IsNotExist(err) {
		// preserve compatibility - pretend we have a default config
		err = nil
		conf = &config.Config{
			Storage: &local.Config{
				Dir: ".", // same dir as config
			},
		}
	}
	if err != nil {
		return nil, err
	}
	// paths of a local storage are relative to the config
	if c, ok := conf.Storage.(*local.Config); ok {
		if c.Dir == "" || !filepath.IsAbs(c.Dir) {
			c.Dir = filepath.Join(opt.Dir, c.Dir)
		}
	}
	s, err := conf.Storage.OpenStorage(context.TODO())
	if err != nil {
		return nil, err
	}
	return New(s)
}

func New(st storage.Storage) (*Storage, error) {
	return &Storage{
		st:    st,
		index: storage.NewBlobIndexer(st),
	}, nil
}

var (
	_ storage.Storage     = (*Storage)(nil)
	_ storage.BlobIndexer = (*Storage)(nil)
)

type Storage struct {
	st    storage.Storage
	index storage.BlobIndexer
}

func (s *Storage) Close() error {
	return s.st.Close()
}

func (s *Storage) SetPin(ctx context.Context, name string, ref types.Ref) error {
	if name == "" {
		name = DefaultPin
	}
	return s.st.SetPin(ctx, name, ref)
}

func (s *Storage) DeletePin(ctx context.Context, name string) error {
	if name == "" {
		name = DefaultPin
	}
	return s.st.DeletePin(ctx, name)
}

func (s *Storage) GetPin(ctx context.Context, name string) (types.Ref, error) {
	if name == "" {
		name = DefaultPin
	}
	return s.st.GetPin(ctx, name)
}

func (s *Storage) GetPinOrRef(ctx context.Context, name string) (types.Ref, error) {
	if !types.IsRef(name) {
		return s.GetPin(ctx, name)
	}
	return types.ParseRef(name)
}

func (s *Storage) IteratePins(ctx context.Context) storage.PinIterator {
	return s.st.IteratePins(ctx)
}

func (s *Storage) FetchBlob(ctx context.Context, ref Ref) (io.ReadCloser, uint64, error) {
	if ref.Empty() {
		// generate empty blobs
		return ioutil.NopCloser(bytes.NewReader(nil)), 0, nil
	}
	rc, sz, err := s.st.FetchBlob(ctx, ref)
	if err == nil {
		rc = storage.VerifyReader(rc, ref)
	}
	return rc, sz, err
}

func (s *Storage) IterateBlobs(ctx context.Context) storage.Iterator {
	return s.st.IterateBlobs(ctx)
}

func (s *Storage) StatBlob(ctx context.Context, ref Ref) (uint64, error) {
	if ref.Empty() {
		return 0, nil
	}
	return s.st.StatBlob(ctx, ref)
}
