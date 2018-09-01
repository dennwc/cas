package xattr

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/pkg/xattr"
)

const userNS = "user."

var endian = binary.LittleEndian

func Get(path, name string) ([]byte, error) {
	return xattr.Get(path, userNS+name)
}

func GetString(path, name string) (string, error) {
	data, err := Get(path, name)
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

func GetTime(path, name string) (time.Time, error) {
	nanos, err := GetUint(path, name)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, int64(nanos)).UTC(), nil
}

func Set(path, name string, data []byte) error {
	return xattr.Set(path, userNS+name, data)
}

func SetString(path, name string, data string) error {
	return Set(path, name, []byte(data))
}

func SetUint(path, name string, v uint64) error {
	var b [8]byte
	endian.PutUint64(b[:], v)
	return Set(path, name, b[:])
}

func SetTime(path, name string, t time.Time) error {
	return SetUint(path, name, uint64(t.UTC().UnixNano()))
}
