package types

import (
	"crypto/md5"
	"encoding/hex"
	"reflect"
	"strconv"

	"github.com/gotd/td/tg"
)

type File struct {
	Location tg.InputFileLocationClass
	FileSize int64
	FileName string
	MimeType string
	ID       int64
	// DCID is the Telegram data centre the file is stored in (0: not known), so its blocks can be
	// fetched over connections straight to it (stream.apiFor).
	DCID int
}

type HashableFileStruct struct {
	FileName string
	FileSize int64
	MimeType string
	FileID   int64
}

func (f *HashableFileStruct) Pack() string {
	hasher := md5.New()
	val := reflect.ValueOf(*f)
	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)

		var fieldValue []byte
		switch field.Kind() {
		case reflect.String:
			fieldValue = []byte(field.String())
		case reflect.Int64:
			fieldValue = []byte(strconv.FormatInt(field.Int(), 10))
		}

		hasher.Write(fieldValue)
	}
	return hex.EncodeToString(hasher.Sum(nil))
}
