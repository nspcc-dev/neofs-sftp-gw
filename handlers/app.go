package handlers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	cid "github.com/nspcc-dev/neofs-api-go/pkg/container/id"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-sdk-go/pool"
	"github.com/pkg/sftp"
	"go.uber.org/zap"
)

const (
	filePathAttribute = "FilePath"
	delimiter         = "/"
)

type (
	// App is the main application structure.
	App struct {
		Log *zap.Logger

		pool      pool.Pool
		sftConfig *SftpServerConfig
	}

	// SftpServerConfig is openssh sftp subsystem params.
	SftpServerConfig struct {
		ReadOnly    bool
		DebugStderr bool
		DebugLevel  string
	}

	// ListerAt is analogue io.ReaderAt for file info list.
	ListerAt []os.FileInfo

	objReader struct {
		file *ObjectInfo
		pool pool.Pool
	}

	objWriter struct {
		file   *ObjectInfo
		pool   pool.Pool
		buffer *bytes.Buffer
	}
)

// NewApp creates handlers (implements sftp.FileReader, sftp.FileWriter, sftp.FileCmder, sftp.FileLister).
func NewApp(conns pool.Pool, l *zap.Logger, sftpConfig *SftpServerConfig) *App {
	return &App{
		pool:      conns,
		Log:       l,
		sftConfig: sftpConfig,
	}
}

func newReader(obj *ObjectInfo, conn pool.Pool) *objReader {
	return &objReader{
		file: obj,
		pool: conn,
	}
}

func newWriter(obj *ObjectInfo, conn pool.Pool) *objWriter {
	return &objWriter{
		file:   obj,
		pool:   conn,
		buffer: bytes.NewBuffer(nil),
	}
}

// ListAt lists files.
func (f ListerAt) ListAt(ls []os.FileInfo, offset int64) (int, error) {
	var n int
	if offset >= int64(len(f)) {
		return 0, io.EOF
	}
	n = copy(ls, f[offset:])
	if n < len(ls) {
		return n, io.EOF
	}
	return n, nil
}

func (a *App) listObjects(ctx context.Context, cnrID *cid.ID) ([]os.FileInfo, error) {
	var result []os.FileInfo

	opts := object.NewSearchFilters()
	opts.AddRootFilter()

	objIds, err := a.pool.SearchObject(ctx, new(client.SearchObjectParams).WithSearchFilters(opts).WithContainerID(cnrID))
	if err != nil {
		return nil, err
	}

	existedFiles := make(map[string]struct{}, len(objIds))

	for _, oid := range objIds {
		obj, err := a.getObjectFile(ctx, newAddress(cnrID, oid))
		if err != nil {
			return nil, err
		}
		if _, ok := existedFiles[obj.Name()]; ok {
			continue
		}
		existedFiles[obj.Name()] = struct{}{}
		result = append(result, obj)
	}
	return result, nil
}

func (a *App) getObjectFile(ctx context.Context, address *object.Address) (*ObjectInfo, error) {
	ops := new(client.ObjectHeaderParams).WithAddress(address).WithAllFields()
	objMeta, err := a.pool.GetObjectHeader(ctx, ops)
	if err != nil {
		return nil, err
	}

	file := &ObjectInfo{
		FileName: address.ObjectID().String(),
		Container: &ContainerInfo{
			CID: address.ContainerID(),
		},
		OID:         address.ObjectID(),
		PayloadSize: int64(objMeta.PayloadSize()),
		Created:     time.Now(),
	}

	for _, attr := range objMeta.Attributes() {
		if attr.Key() == object.AttributeTimestamp {
			unix, err := strconv.ParseInt(attr.Value(), 10, 64)
			if err == nil {
				file.Created = time.Unix(unix, 0)
			}
		}
		if attr.Key() == object.AttributeFileName {
			file.FileName = attr.Value()
		}
		if attr.Key() == filePathAttribute {
			file.FilePath = attr.Value()
		}
	}

	return file, nil
}

func (a *App) getObjectFileByName(ctx context.Context, cnrID *cid.ID, name string) (*ObjectInfo, error) {
	filters := object.NewSearchFilters()
	filters.AddRootFilter()
	filters.AddFilter(object.AttributeFileName, name, object.MatchStringEqual)

	params := new(client.SearchObjectParams).WithSearchFilters(filters).WithContainerID(cnrID)
	objIds, err := a.pool.SearchObject(ctx, params)
	if err != nil {
		return nil, err
	}

	if len(objIds) == 0 {
		return nil, fmt.Errorf("not found")
	}

	return a.getObjectFile(ctx, newAddress(cnrID, objIds[0]))
}

func (a *App) getContainer(ctx context.Context, cnrID *cid.ID) (*ContainerInfo, error) {
	ctnr, err := a.pool.GetContainer(ctx, cnrID)
	if err != nil {
		return nil, err
	}

	file := &ContainerInfo{
		FileName: cnrID.String(),
		CID:      cnrID,
		Created:  time.Now(),
	}

	for _, attr := range ctnr.Attributes() {
		if attr.Key() == container.AttributeTimestamp {
			unix, err := strconv.ParseInt(attr.Value(), 10, 64)
			if err == nil {
				file.Created = time.Unix(unix, 0)
			}
		}
		if attr.Key() == container.AttributeName {
			file.FileName = attr.Value()
		}
	}

	return file, nil
}

func (a *App) listContainers(ctx context.Context) ([]os.FileInfo, error) {
	var result []os.FileInfo
	containers, err := a.pool.ListContainers(ctx, a.pool.OwnerID())
	if err != nil {
		return nil, err
	}

	existedFiles := make(map[string]struct{}, len(containers))

	for _, CID := range containers {
		ctnr, err := a.getContainer(ctx, CID)
		if err != nil {
			return nil, err
		}

		if _, ok := existedFiles[ctnr.Name()]; ok {
			continue
		}
		existedFiles[ctnr.Name()] = struct{}{}
		result = append(result, ctnr)
	}
	return result, nil
}

func (a *App) getContainers(ctx context.Context) ([]*ContainerInfo, error) {
	var result []*ContainerInfo
	containers, err := a.pool.ListContainers(ctx, a.pool.OwnerID())
	if err != nil {
		return nil, err
	}

	existedFiles := make(map[string]struct{}, len(containers))

	for _, CID := range containers {
		ctnr, err := a.getContainer(ctx, CID)
		if err != nil {
			return nil, err
		}

		if _, ok := existedFiles[ctnr.Name()]; ok {
			continue
		}
		existedFiles[ctnr.Name()] = struct{}{}
		result = append(result, ctnr)
	}
	return result, nil
}

func (a *App) getContainerByName(ctx context.Context, name string) (*ContainerInfo, error) {
	CID := cid.New()
	if err := CID.Parse(name); err == nil {
		return a.getContainer(ctx, CID)
	}

	containers, err := a.getContainers(ctx)
	if err != nil {
		return nil, err
	}

	for _, ctnr := range containers {
		if ctnr.Name() == name {
			return ctnr, nil
		}
	}

	return nil, fmt.Errorf("not found")
}

func (a *App) listPath(ctx context.Context, path string) ([]os.FileInfo, error) {
	path = strings.TrimPrefix(path, delimiter)
	if path == "" {
		return a.listContainers(ctx)
	}

	ctnr, err := a.getContainerByName(ctx, path)
	if err != nil {
		return nil, err
	}

	return a.listObjects(ctx, ctnr.CID)
}

func (a *App) getFileStat(ctx context.Context, path string) (os.FileInfo, error) {
	path = strings.TrimPrefix(path, delimiter)
	if path == "" {
		return &ContainerInfo{FileName: delimiter, Created: time.Now()}, nil
	}
	split := strings.Split(path, delimiter)

	ctnr, err := a.getContainerByName(ctx, split[0])
	if err != nil {
		return nil, err
	}

	if len(split) == 2 && len(split[1]) > 0 {
		oid := object.NewID()
		if err := oid.Parse(split[1]); err != nil {
			return nil, err
		}

		obj, err := a.getObjectFile(ctx, newAddress(ctnr.CID, oid))
		if err != nil {
			return nil, err
		}
		return obj, nil
	}

	return ctnr, nil
}

func (a *App) deleteNeofsFile(ctx context.Context, path string) error {
	path = strings.TrimPrefix(path, delimiter)
	split := strings.Split(path, delimiter)

	cntr, err := a.getContainerByName(ctx, split[0])
	if err != nil {
		return err
	}
	if len(split) == 2 && split[1] != "" {
		obj, err := a.getObjectFileByName(ctx, cntr.CID, split[1])
		if err != nil {
			return err
		}
		return a.deleteObject(ctx, newAddress(cntr.CID, obj.OID))
	}

	return a.deleteContainer(ctx, cntr.CID)
}

func (a *App) deleteObject(ctx context.Context, address *object.Address) error {
	params := new(client.DeleteObjectParams).WithAddress(address)
	return a.pool.DeleteObject(ctx, params)
}

func (a *App) deleteContainer(ctx context.Context, cnrID *cid.ID) error {
	return a.pool.DeleteContainer(ctx, cnrID)
}

// Filecmd called for Methods: Setstat, Rename, Rmdir, Mkdir, Link, Symlink, Remove.
func (a *App) Filecmd(r *sftp.Request) error {
	if a.sftConfig.ReadOnly {
		return sftp.ErrSSHFxPermissionDenied
	}
	switch r.Method {
	case "Mkdir":
	case "Remove", "Rmdir":
		err := a.deleteNeofsFile(context.TODO(), r.Filepath)
		return err
	}

	return nil
}

// Filewrite prepares io.WriterAt to upload files.
// Called for Methods: Put, Open.
func (a *App) Filewrite(r *sftp.Request) (io.WriterAt, error) {
	if a.sftConfig.ReadOnly {
		return nil, sftp.ErrSSHFxPermissionDenied
	}
	trimmed := strings.TrimPrefix(r.Filepath, delimiter)
	split := strings.Split(trimmed, delimiter)
	ctnr, err := a.getContainerByName(context.TODO(), split[0])
	if err != nil {
		return nil, err
	}

	obj := &ObjectInfo{
		FileName:  strings.TrimPrefix(trimmed, split[0]+delimiter),
		Container: ctnr,
	}

	return newWriter(obj, a.pool), nil
}

// Fileread prepares io.ReaderAt to download file.
// Called for Methods: Get.
func (a *App) Fileread(r *sftp.Request) (io.ReaderAt, error) {
	file, err := a.getFileStat(context.TODO(), r.Filepath)
	if err != nil {
		return nil, err
	}

	obj, ok := file.(*ObjectInfo)
	if !ok {
		return nil, fmt.Errorf("couldn't get file stat")
	}

	return newReader(obj, a.pool), nil
}

// Filelist returns files information.
// Called for Methods: List, Stat, Readlink.
func (a *App) Filelist(r *sftp.Request) (sftp.ListerAt, error) {
	switch r.Method {
	case "List":
		files, err := a.listPath(context.TODO(), r.Filepath)
		if err != nil {
			return nil, err
		}
		return ListerAt(files), nil
	case "Stat":
		stat, err := a.getFileStat(context.TODO(), r.Filepath)
		if err != nil {
			return nil, err
		}
		return ListerAt([]os.FileInfo{stat}), nil
	case "Readlink":
	}

	return nil, errors.New("unsupported")
}

func newAddress(cid *cid.ID, oid *object.ID) *object.Address {
	address := object.NewAddress()
	address.SetContainerID(cid)
	address.SetObjectID(oid)
	return address
}

func (w *objWriter) Close() error {
	attributes := make([]*object.Attribute, 0, 2)
	filename := object.NewAttribute()
	filename.SetKey(object.AttributeFileName)
	filename.SetValue(w.file.Name())

	createdAt := object.NewAttribute()
	createdAt.SetKey(object.AttributeTimestamp)
	createdAt.SetValue(strconv.FormatInt(time.Now().UTC().Unix(), 10))

	attributes = append(attributes, filename, createdAt)

	raw := object.NewRaw()
	raw.SetOwnerID(w.pool.OwnerID())
	raw.SetContainerID(w.file.Container.CID)
	raw.SetAttributes(attributes...)

	ops := new(client.PutObjectParams).WithObject(raw.Object()).WithPayloadReader(w.buffer)
	_, err := w.pool.PutObject(context.TODO(), ops)
	return err
}

func (w *objWriter) WriteAt(p []byte, off int64) (n int, err error) {
	if off != int64(w.buffer.Len()) {
		return 0, fmt.Errorf("unsupported")
	}
	return w.buffer.Write(p)
}

func (r *objReader) ReadAt(b []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, errors.New("objReader.ReadAt: negative offset")
	}

	if off >= r.file.Size() {
		return 0, io.EOF
	}

	length := uint64(len(b))
	availableLength := uint64(r.file.Size() - off)
	if length > availableLength {
		length = availableLength
	}
	rang := object.NewRange()
	rang.SetLength(length)
	rang.SetOffset(uint64(off))

	ops := new(client.RangeDataParams).WithAddress(newAddress(r.file.Container.CID, r.file.OID)).WithRange(rang)
	data, err := r.pool.ObjectPayloadRangeData(context.TODO(), ops)
	if err != nil {
		return 0, err
	}

	n = copy(b, data)
	if n < len(b) {
		err = io.EOF
	}
	return
}
