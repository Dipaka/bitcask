package bitcask

import (
	"os"
	"path/filepath"
	"strconv"
)

type file struct {
	id int64
	fd *os.File

	off int64
}

const (
	appendFilePrefix = "a"
	mergedFilePrefix = "m"
)

func openReadFile(dirname string, id int64) (*file, error) {
	f := new(file)
	f.id = id

	path := filepath.Join(dirname, appendFilePrefix+strconv.FormatInt(id, 10))
	var err error
	f.fd, err = os.Open(path)
	if err != nil {
		return nil, err
	}

	f.off, err = f.fd.Seek(0, os.SEEK_END)
	if err != nil {
		f.fd.Close()
		return nil, err
	}

	return f, nil
}

func openAppendFile(dirname string, id int64) (*file, error) {
	f := new(file)
	f.id = id

	path := filepath.Join(dirname, appendFilePrefix+strconv.FormatInt(id, 10))
	var err error
	f.fd, err = os.OpenFile(path, os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}

	f.off, err = f.fd.Seek(0, os.SEEK_END)
	if err != nil {
		f.fd.Close()
		return nil, err
	}

	return f, nil
}

func createAppendFile(dirname string, id int64) (*file, error) {
	f := new(file)
	f.id = id

	path := filepath.Join(dirname, appendFilePrefix+strconv.FormatInt(id, 10))
	var err error
	f.fd, err = os.Create(path)
	if err != nil {
		return nil, err
	}

	f.off = 0

	return f, nil
}

func (f *file) appendRecord(rec *record) (int64, error) {
	nbytes, err := f.fd.WriteAt([]byte(rec.toString()), f.off)
	if err != nil {
		return 0, err
	}

	valpos := f.off
	f.off += int64(nbytes)

	return valpos, nil
}

func (f *file) size() int64 {
	return f.off
}
