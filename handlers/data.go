package handlers

import (
	"io/fs"
	"time"

	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

type (
	// ContainerInfo contains neofs container data.
	// Implements fs.FileInfo.
	ContainerInfo struct {
		CID      cid.ID
		FileName string
		Created  time.Time
	}

	// ObjectInfo contains neofs object data.
	// Implements fs.FileInfo.
	ObjectInfo struct {
		Container   *ContainerInfo
		ObjectID    oid.ID
		FilePath    string
		FileName    string
		PayloadSize int64
		Created     time.Time
	}
)

func (t *ContainerInfo) Name() string {
	return t.FileName
}

func (t *ContainerInfo) Size() int64 {
	return 0
}

func (t *ContainerInfo) Mode() fs.FileMode {
	return fs.ModePerm | fs.ModeDir
}

func (t *ContainerInfo) ModTime() time.Time {
	return t.Created
}

func (t *ContainerInfo) IsDir() bool {
	return true
}

func (t *ContainerInfo) Sys() interface{} {
	return nil
}

func (t *ObjectInfo) Name() string {
	return t.FileName
}

func (t *ObjectInfo) Size() int64 {
	return t.PayloadSize
}

func (t *ObjectInfo) Mode() fs.FileMode {
	return fs.ModePerm
}

func (t *ObjectInfo) ModTime() time.Time {
	return t.Created
}

func (t *ObjectInfo) IsDir() bool {
	return false
}

func (t *ObjectInfo) Sys() interface{} {
	return nil
}
