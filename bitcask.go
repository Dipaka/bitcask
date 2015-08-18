package bitcask

import (
	"errors"
	"io/ioutil"
	"path/filepath"
	"strconv"
)

const (
	PutNullKeyError = "PutNullKeyError"
	PutNullValError = "PutNullValError"
)

type Options struct {
}

type Handle struct {
	dirname    string
	keydir     *keydir
	files      map[int64]*file
	activefile *file
}

func Open(dirname string) (*Handle, error) {
	h := new(Handle)
	var err error
	h.dirname, err = filepath.Abs(dirname)
	if err != nil {
		return nil, err
	}

	h.keydir, err = newKeyDir(dirname)
	if err != nil {
		return nil, err
	}

	h.files = make(map[int64]*file)
	infos, err := ioutil.ReadDir(dirname)
	var max int64 = -1
	for _, info := range infos {
		name := info.Name()
		if prefix := name[:1]; prefix == appendFilePrefix {
			id, err := strconv.ParseInt(name[1:], 10, 64)
			if err != nil {
				continue
			}

			f, err := openReadFile(dirname, id)
			if err != nil {
				//TODO deal with error
				continue
			}
			h.files[id] = f

			fsz := f.size()
			var valpos int64 = 0
			for valpos < fsz {
				rec, err := newRecordFromFile(f, valpos)
				if err != nil {
					//TODO
					break
				}

				msg := new(metamsg)
				msg.id = id
				msg.valsz = rec.valsz
				msg.valpos = valpos
				msg.tstamp = rec.tstamp
				h.keydir.put(rec.key, msg)

				valpos += rec.size()
			}

			if id > max {
				max = id
			}
		} else if prefix == mergedFilePrefix {
			//TODO
		}
	}

	if max != -1 {
		h.activefile, err = openAppendFile(dirname, max)
		if err != nil {
			return nil, err
		}
	} else {
		h.activefile, err = createAppendFile(dirname, 0)
		if err != nil {
			return nil, err
		}
		h.files[max] = h.activefile
	}

	return h, nil
}

func OpenWithOptions(dirname string, Options *Options) (*Handle, error) {
	return Open(dirname)
}

func Merge(dirname string) error {
	return nil
}

func (h *Handle) Close() error {
	for _, f := range h.files {
		f.fd.Close()
	}

	h.activefile.fd.Close()

	return nil
}

func (h *Handle) Sync() error {
	return h.activefile.fd.Sync()
}

func (h *Handle) Get(key string) (string, error) {
	msg, err := h.keydir.get(key)
	if err != nil {
		return "", err
	}

	f, ok := h.files[msg.id]
	if !ok {
		f, err = openAppendFile(h.dirname, msg.id)
		if err != nil {
			return "", err
		}
		h.files[msg.id] = f
	}
	rec, err := newRecordFromFile(f, msg.valpos)
	if err != nil {
		return "", err
	}

	return rec.val, nil
}

func (h *Handle) Put(key, val string) error {
	if len(key) == 0 {
		return errors.New(PutNullKeyError)
	}

	if len(val) == 0 {
		return errors.New(PutNullValError)
	}

	return h.doPut(key, val)
}

func (h *Handle) Delete(key string) error {
	err := h.doPut(key, "")
	if err != nil {
		delete(h.keydir.msgs, key)
	}

	return nil
}

func (h *Handle) Keys() []string {
	return h.keydir.allKeys()
}

/* ========================================================================== */
/*                       Private auxiliary API below                          */
/* ========================================================================== */
func (h *Handle) doPut(key, val string) error {
	rec, err := newRecordFromKV(key, val)
	if err != nil {
		return err
	}

	valpos, err := h.activefile.appendRecord(rec)
	if err != nil {
		return err
	}

	msg := &metamsg{
		id:     h.activefile.id,
		valsz:  rec.valsz,
		valpos: valpos,
		tstamp: rec.tstamp,
	}
	h.keydir.put(key, msg)

	return nil
}
