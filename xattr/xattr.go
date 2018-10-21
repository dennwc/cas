package xattr

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/pkg/xattr"
)

const userNS = "user."

var endian = binary.LittleEndian

var ErrNotSet = errors.New("xattr not set")

func Get(path, name string) ([]byte, error) {
	data, err := xattr.Get(path, userNS+name)
	if e, ok := err.(*xattr.Error); ok && e.Err == xattr.ENOATTR {
		return nil, ErrNotSet
	} else if err != nil {
		return nil, err
	}
	return data, nil
}

func GetF(f *os.File, name string) ([]byte, error) {
	data, err := xattr.FGet(f, userNS+name)
	if e, ok := err.(*xattr.Error); ok && e.Err == xattr.ENOATTR {
		return nil, ErrNotSet
	} else if err != nil {
		return nil, err
	}
	return data, nil
}

func GetString(path, name string) (string, error) {
	data, err := Get(path, name)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func GetStringF(f *os.File, name string) (string, error) {
	data, err := GetF(f, name)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func GetUint(path, name string) (uint64, error) {
	data, err := Get(path, name)
	if err != nil {
		return 0, err
	} else if len(data) != 8 {
		return 0, fmt.Errorf("xattr: wrong int format")
	}
	return endian.Uint64(data), nil
}

func GetUintF(f *os.File, name string) (uint64, error) {
	data, err := GetF(f, name)
	if err != nil {
		return 0, err
	} else if len(data) != 8 {
		return 0, fmt.Errorf("xattr: wrong int format")
	}
	return endian.Uint64(data), nil
}

func GetTime(path, name string) (time.Time, error) {
	nanos, err := GetUint(path, name)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, int64(nanos)).UTC(), nil
}

func GetTimeF(f *os.File, name string) (time.Time, error) {
	nanos, err := GetUintF(f, name)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, int64(nanos)).UTC(), nil
}

func Set(path, name string, data []byte) error {
	return xattr.Set(path, userNS+name, data)
}

func SetF(f *os.File, name string, data []byte) error {
	return xattr.FSet(f, userNS+name, data)
}

func SetString(path, name string, data string) error {
	return Set(path, name, []byte(data))
}

func SetStringF(f *os.File, name string, data string) error {
	return SetF(f, name, []byte(data))
}

func SetUint(path, name string, v uint64) error {
	var b [8]byte
	endian.PutUint64(b[:], v)
	return Set(path, name, b[:])
}

func SetUintF(f *os.File, name string, v uint64) error {
	var b [8]byte
	endian.PutUint64(b[:], v)
	return SetF(f, name, b[:])
}

func SetTime(path, name string, t time.Time) error {
	return SetUint(path, name, uint64(t.UTC().UnixNano()))
}

func SetTimeF(f *os.File, name string, t time.Time) error {
	return SetUintF(f, name, uint64(t.UTC().UnixNano()))
}
