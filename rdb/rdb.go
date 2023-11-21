package rdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	Magic = "REDIS"
)
const (
	OpAux      = 0xFA
	OpResizeDB = 0xFB
	OpExpireMs = 0xFC
	OpExpire   = 0xFD
	OpSelectDB = 0xFE
	OpEOF      = 0xFF
)

type KeyValue struct {
	Value     string
	HasExpiry bool
	Expires   time.Time
}

type RDBFile struct {
	path      string
	data      []byte
	datastore map[string]*KeyValue
}

type LengthEncoding struct {
	length    int32
	isEncoded bool
}

func (r *RDBFile) readLength(rd io.Reader) (LengthEncoding, error) {
	b := make([]byte, 1)
	if _, err := rd.Read(b); err != nil {
		return LengthEncoding{}, err
	}
	msb := (b[0] & 0xC0) >> 6 // first 2 bits
	val := int32(b[0] & 0x3F) // last 6 bits
	switch msb {
	case 0:
		return LengthEncoding{length: val, isEncoded: false}, nil
	case 1:
		nb := make([]byte, 1)
		if _, err := rd.Read(b); err != nil {
			return LengthEncoding{}, err
		}
		return LengthEncoding{length: int32((val << 8) | int32(nb[0])), isEncoded: false}, nil
	case 2:
		nb := make([]byte, 4)
		if _, err := rd.Read(b); err != nil {
			return LengthEncoding{}, err
		}
		return LengthEncoding{length: int32(binary.LittleEndian.Uint32(nb)), isEncoded: false}, nil
	case 3:
		return LengthEncoding{length: val, isEncoded: true}, nil
	}
	return LengthEncoding{}, errors.New("parse: length: invalid length encoding")
}

func (r *RDBFile) readString(rd io.Reader) (string, error) {
	l, err := r.readLength(rd)
	if err != nil {
		return "", err
	}
	if !l.isEncoded {
		b := make([]byte, l.length)
		if _, err := rd.Read(b); err != nil {
			return "", err
		}
		return string(b), nil
	}
	switch l.length {
	case 0:
		var enc int8
		if err := binary.Read(rd, binary.LittleEndian, &enc); err != nil {
			return "", err
		}
		return strconv.Itoa(int(enc)), nil
	case 1:
		fmt.Println("16 bit")
		b := make([]byte, 2)
		if _, err := rd.Read(b); err != nil {
			return "", err
		}
		var enc int16
		if err := binary.Read(rd, binary.LittleEndian, &enc); err != nil {
			return "", err
		}
		return strconv.Itoa(int(enc)), nil
	case 2:
		fmt.Println("32 bit")
		b := make([]byte, 4)
		if _, err := rd.Read(b); err != nil {
			return "", err
		}
		var enc int32
		if err := binary.Read(rd, binary.LittleEndian, &enc); err != nil {
			return "", err
		}
		return strconv.Itoa(int(enc)), nil
	case 3:
		return "", errors.New("parse: string: LZMA compressed string")
	}
	return "", errors.New("parse: string: invalid encoding")
}

func (r *RDBFile) Parse() error {
	if r == nil {
		fmt.Println("protocol: rdb: parse: an error was observed while loading the RDB file")
		return nil
	}
	reader := bufio.NewReader(bytes.NewBuffer(r.data))
	header := make([]byte, 5)
	if _, err := reader.Read(header); err != nil {
		return err
	}
	if strings.ToUpper(string(header)) != Magic {
		return errors.New("protocol: rdb: parse: invalid RDB file")
	}
	version := make([]byte, 4)
	if _, err := reader.Read(version); err != nil {
		return err
	}
	versionInt, _ := strconv.Atoi(string(version))
	fmt.Printf("parsing RDB file with version %d\n", versionInt)
	hasReachedEOF := false
	nextKeyHasExpiry := false
	var expiryValue int64

	for !hasReachedEOF {

		opcode := make([]byte, 1)
		if _, err := reader.Read(opcode); err != nil {
			return err
		}
		switch opcode[0] {
		case OpAux:
			fmt.Println("read instruction aux")
			key, err := r.readString(reader)
			if err != nil {
				return err
			}
			value, err := r.readString(reader)
			if err != nil {
				return err
			}
			fmt.Printf("key: %s, value: %s\n", string(key), string(value))
		case OpResizeDB:
			fmt.Println("read instruction resizedb")
			dbHashTableSize, err := r.readLength(reader)
			if err != nil {
				return err
			}
			expiryHashTableSize, err := r.readLength(reader)
			if err != nil {
				return err
			}
			fmt.Printf("db hash table size: %d, expiry hash table size: %d\n", dbHashTableSize.length, expiryHashTableSize.length)
		case OpExpireMs:
			fmt.Println("read instruction expirems")
			if err := binary.Read(reader, binary.LittleEndian, &expiryValue); err != nil {
				return err
			}
			nextKeyHasExpiry = true
		case OpExpire:
			fmt.Println("read instruction expire")
		case OpSelectDB:
			fmt.Println("read instruction selectdb")
			dbNumber, err := r.readLength(reader)
			if err != nil {
				return err
			}
			fmt.Printf("selected db number: %d\n", dbNumber.length)
		case OpEOF:
			fmt.Println("read instruction EOF")
			hasReachedEOF = true
		default:
			valueType := opcode[0]
			var value string
			key, err := r.readString(reader)
			if err != nil {
				return err
			}
			switch valueType {
			case 0:
				readValue, err := r.readString(reader)
				if err != nil {
					return err
				}
				value = readValue
			default:
				return errors.New("parse: KV-pair: invalid value type")
			}
			kv := KeyValue{
				HasExpiry: false,
				Value:     value,
			}
			if nextKeyHasExpiry {
				t := time.UnixMilli(expiryValue)
				kv.HasExpiry = true
				kv.Expires = t
				nextKeyHasExpiry = false
				fmt.Printf("key-value read: <has expiry> <%s>: %s (exp: %s)\n", key, value, t)
			} else {
				fmt.Printf("key-value read: <%s>: %s\n", key, value)
			}

			r.datastore[key] = &kv
		}
	}
	return nil
}
func (r *RDBFile) GetDB() map[string]*KeyValue {
	if r == nil {
		return map[string]*KeyValue{}
	}
	return r.datastore
}
func NewRDBFileFromPath(p string) (*RDBFile, error) {
	b, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	return &RDBFile{path: p, data: b, datastore: map[string]*KeyValue{}}, nil
}
