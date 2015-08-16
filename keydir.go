package bitcask

import (
	"errors"
)

const (
	keyNotFound = "keyNotFound"
)

type keydir struct {
	msgs map[string]*metamsg
	keys []string
}

type metamsg struct {
	id     int64
	valsz  uint32
	valpos int64
	tstamp uint32
}

func newKeyDir(dirname string) (*keydir, error) {
	d := new(keydir)
	d.msgs = make(map[string]*metamsg)
	d.keys = make([]string, 0, 1024)

	return d, nil
}

func (d *keydir) put(key string, msg *metamsg) error {
	_, ok := d.msgs[key]
	if !ok {
		d.keys = append(d.keys, key)
	}

	d.msgs[key] = msg

	return nil
}

func (d *keydir) get(key string) (*metamsg, error) {
	msg, ok := d.msgs[key]
	if !ok {
		return nil, errors.New(keyNotFound)
	}
	return msg, nil
}

func (d *keydir) allKeys() []string {
	return d.keys
}
