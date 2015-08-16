package bitcask

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"strconv"
	"time"
)

const (
	hrdsz = 13
)

type record struct {
	crc32  uint32
	tstamp uint32
	keysz  uint8
	valsz  uint32
	key    string
	val    string
}

func newRecordFromKV(key, val string) (*record, error) {
	rec := new(record)

	rec.crc32 = 0
	rec.tstamp = uint32(time.Now().Unix())
	rec.keysz = uint8(len(key))
	rec.valsz = uint32(len(val))
	rec.key = key
	rec.val = val

	//TODO deal with error below
	crcbuf := make([]byte, 0, hrdsz+int(rec.keysz)+int(rec.valsz))
	buf := bytes.NewBuffer(crcbuf)
	binary.Write(buf, binary.LittleEndian, rec.tstamp)
	binary.Write(buf, binary.LittleEndian, rec.keysz)
	binary.Write(buf, binary.LittleEndian, rec.valsz)
	buf.Write([]byte(key))
	buf.Write([]byte(val))

	rec.crc32 = crc32.ChecksumIEEE(buf.Bytes())

	return rec, nil
}

func newRecordFromFile(f *file, off int64) (*record, error) {
	hrd := make([]byte, hrdsz)
	_, err := f.fd.ReadAt(hrd, off)
	if err != nil {
		return nil, err
	}

	rec := new(record)
	buf := bytes.NewReader(hrd)

	//TODO deal with error below
	binary.Read(buf, binary.LittleEndian, &rec.crc32)
	binary.Read(buf, binary.LittleEndian, &rec.tstamp)
	binary.Read(buf, binary.LittleEndian, &rec.keysz)
	binary.Read(buf, binary.LittleEndian, &rec.valsz)

	iobuf := make([]byte, rec.keysz)
	//TODO make key/value parsing in one for-loop
	//read key
	nread := 0
	for {
		offset := off + int64(hrdsz) + int64(nread)
		nbytes, err := f.fd.ReadAt(iobuf[nread:], offset)
		if err != nil {
			return nil, err
		}

		nread += nbytes
		if nread == int(rec.keysz) {
			rec.key = string(iobuf[:nread])
			break
		}
	}
	// read value
	iobuf = make([]byte, rec.valsz)
	nread = 0
	for {
		offset := off + int64(hrdsz) + int64(rec.keysz) + int64(nread)
		nbytes, err := f.fd.ReadAt(iobuf[nread:], offset)
		if err != nil {
			return nil, err
		}

		nread += nbytes
		if nread == int(rec.valsz) {
			rec.val = string(iobuf[:nread])
			break
		}
	}

	return rec, nil
}

func (rec *record) size() int64 {
	return int64(hrdsz) + int64(rec.keysz) + int64(rec.valsz)
}

func (rec *record) toString() string {
	buf := bytes.NewBuffer(make([]byte, 0, rec.size()))
	binary.Write(buf, binary.LittleEndian, rec.crc32)
	binary.Write(buf, binary.LittleEndian, rec.tstamp)
	binary.Write(buf, binary.LittleEndian, rec.keysz)
	binary.Write(buf, binary.LittleEndian, rec.valsz)
	buf.WriteString(rec.key)
	buf.WriteString(rec.val)
	return string(buf.Bytes())
}

func (rec *record) toReadableString() string {
	buf := bytes.NewBuffer(make([]byte, 0, 1024))
	buf.WriteString("[record]")
	buf.WriteString(" crc32 ")
	buf.WriteString(strconv.Itoa(int(rec.crc32)))
	buf.WriteString(" tstamp ")
	buf.WriteString(strconv.Itoa(int(rec.tstamp)))
	buf.WriteString(" keysz ")
	buf.WriteString(strconv.Itoa(int(rec.keysz)))
	buf.WriteString(" valsz ")
	buf.WriteString(strconv.Itoa(int(rec.valsz)))
	buf.WriteString(" key ")
	buf.WriteString(rec.key)
	buf.WriteString(" val ")
	buf.WriteString(rec.val)

	return string(buf.Bytes())
}
