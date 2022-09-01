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

	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/pool"
	"github.com/nspcc-dev/neofs-sdk-go/user"
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

		pool      *pool.Pool
		owner     *user.ID
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
		ctx  context.Context
		file *ObjectInfo
		pool *pool.Pool
	}

	objWriter struct {
		ctx    context.Context
		file   *ObjectInfo
		pool   *pool.Pool
		owner  *user.ID
		buffer *bytes.Buffer
	}
)

// NewApp creates handlers (implements sftp.FileReader, sftp.FileWriter, sftp.FileCmder, sftp.FileLister).
func NewApp(conns *pool.Pool, owner *user.ID, l *zap.Logger, sftpConfig *SftpServerConfig) *App {
	return &App{
		pool:      conns,
		owner:     owner,
		Log:       l,
		sftConfig: sftpConfig,
	}
}

func newReader(ctx context.Context, obj *ObjectInfo, conn *pool.Pool) *objReader {
	return &objReader{
		ctx:  ctx,
		file: obj,
		pool: conn,
	}
}

func newWriter(ctx context.Context, obj *ObjectInfo, conn *pool.Pool, ownerID *user.ID) *objWriter {
	return &objWriter{
		ctx:    ctx,
		file:   obj,
		pool:   conn,
		owner:  ownerID,
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

func (a *App) listObjects(ctx context.Context, cnrID cid.ID) ([]os.FileInfo, error) {
	var result []os.FileInfo

	filters := object.NewSearchFilters()
	filters.AddRootFilter()

	var prm pool.PrmObjectSearch
	prm.SetContainerID(cnrID)
	prm.SetFilters(filters)

	res, err := a.pool.SearchObjects(ctx, prm)
	if err != nil {
		return nil, fmt.Errorf("init searching: %w", err)
	}
	defer res.Close()

	existedFiles := make(map[string]struct{})

	var inErr error
	var obj *ObjectInfo

	err = res.Iterate(func(id oid.ID) bool {
		obj, inErr = a.getObjectFile(ctx, newAddress(cnrID, id))
		if err != nil {
			return true
		}
		if _, ok := existedFiles[obj.Name()]; ok {
			return false
		}
		existedFiles[obj.Name()] = struct{}{}
		result = append(result, obj)
		return false
	})
	if err == nil {
		err = inErr
	}

	return result, err
}

func (a *App) getObjectFile(ctx context.Context, address oid.Address) (*ObjectInfo, error) {
	var prm pool.PrmObjectHead
	prm.SetAddress(address)
	objMeta, err := a.pool.HeadObject(ctx, prm)
	if err != nil {
		return nil, err
	}

	file := &ObjectInfo{
		FileName: address.Object().String(),
		Container: &ContainerInfo{
			CID: address.Container(),
		},
		ObjectID:    address.Object(),
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

func (a *App) getObjectFileByName(ctx context.Context, cnrID cid.ID, name string) (*ObjectInfo, error) {
	filters := object.NewSearchFilters()
	filters.AddRootFilter()
	filters.AddFilter(object.AttributeFileName, name, object.MatchStringEqual)

	var prm pool.PrmObjectSearch
	prm.SetContainerID(cnrID)
	prm.SetFilters(filters)

	res, err := a.pool.SearchObjects(ctx, prm)
	if err != nil {
		return nil, fmt.Errorf("init searching: %w", err)
	}
	defer res.Close()

	var objID *oid.ID
	err = res.Iterate(func(id oid.ID) bool {
		objID = &id
		return true
	})
	if err != nil {
		return nil, err
	}

	if objID == nil {
		return nil, fmt.Errorf("not found")
	}

	return a.getObjectFile(ctx, newAddress(cnrID, *objID))
}

func (a *App) getContainer(ctx context.Context, cnrID cid.ID) (*ContainerInfo, error) {
	var prm pool.PrmContainerGet
	prm.SetContainerID(cnrID)
	cnr, err := a.pool.GetContainer(ctx, prm)
	if err != nil {
		return nil, err
	}

	file := &ContainerInfo{
		FileName: cnrID.EncodeToString(),
		CID:      cnrID,
		Created:  time.Now(),
	}

	if cnrName := container.Name(cnr); len(cnrName) != 0 {
		file.FileName = cnrName
	}

	if createdTime := container.CreatedAt(cnr); createdTime.IsZero() {
		file.Created = createdTime
	}

	return file, nil
}

func (a *App) listContainers(ctx context.Context) ([]os.FileInfo, error) {
	var result []os.FileInfo

	var prm pool.PrmContainerList
	prm.SetOwnerID(*a.owner)

	containers, err := a.pool.ListContainers(ctx, prm)
	if err != nil {
		return nil, err
	}

	existedFiles := make(map[string]struct{}, len(containers))

	for _, CID := range containers {
		cnr, err := a.getContainer(ctx, CID)
		if err != nil {
			return nil, err
		}

		if _, ok := existedFiles[cnr.Name()]; ok {
			continue
		}
		existedFiles[cnr.Name()] = struct{}{}
		result = append(result, cnr)
	}
	return result, nil
}

func (a *App) getContainers(ctx context.Context) ([]*ContainerInfo, error) {
	var result []*ContainerInfo

	var prm pool.PrmContainerList
	prm.SetOwnerID(*a.owner)

	containers, err := a.pool.ListContainers(ctx, prm)
	if err != nil {
		return nil, err
	}

	existedFiles := make(map[string]struct{}, len(containers))

	for _, CID := range containers {
		cnr, err := a.getContainer(ctx, CID)
		if err != nil {
			return nil, err
		}

		if _, ok := existedFiles[cnr.Name()]; ok {
			continue
		}
		existedFiles[cnr.Name()] = struct{}{}
		result = append(result, cnr)
	}
	return result, nil
}

func (a *App) getContainerByName(ctx context.Context, name string) (*ContainerInfo, error) {
	var cnrID cid.ID
	if err := cnrID.DecodeString(name); err == nil {
		return a.getContainer(ctx, cnrID)
	}

	containers, err := a.getContainers(ctx)
	if err != nil {
		return nil, err
	}

	for _, cnr := range containers {
		if cnr.Name() == name {
			return cnr, nil
		}
	}

	return nil, fmt.Errorf("not found")
}

func (a *App) listPath(ctx context.Context, path string) ([]os.FileInfo, error) {
	path = strings.TrimPrefix(path, delimiter)
	if path == "" {
		return a.listContainers(ctx)
	}

	cnr, err := a.getContainerByName(ctx, path)
	if err != nil {
		return nil, err
	}

	return a.listObjects(ctx, cnr.CID)
}

func (a *App) getFileStat(ctx context.Context, path string) (os.FileInfo, error) {
	path = strings.TrimPrefix(path, delimiter)
	if path == "" {
		return &ContainerInfo{FileName: delimiter, Created: time.Now()}, nil
	}
	split := strings.Split(path, delimiter)

	cnr, err := a.getContainerByName(ctx, split[0])
	if err != nil {
		return nil, err
	}

	if len(split) == 2 && len(split[1]) > 0 {
		var id oid.ID
		if err = id.DecodeString(split[1]); err != nil {
			return nil, err
		}

		obj, err := a.getObjectFile(ctx, newAddress(cnr.CID, id))
		if err != nil {
			return nil, err
		}
		return obj, nil
	}

	return cnr, nil
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

		var prm pool.PrmObjectDelete
		prm.SetAddress(newAddress(cntr.CID, obj.ObjectID))

		return a.pool.DeleteObject(ctx, prm)
	}

	return a.deleteContainer(ctx, cntr.CID)
}

func (a *App) deleteContainer(ctx context.Context, cnrID cid.ID) error {
	var prm pool.PrmContainerDelete
	prm.SetContainerID(cnrID)
	return a.pool.DeleteContainer(ctx, prm)
}

// Filecmd called for Methods: Setstat, Rename, Rmdir, Mkdir, Link, Symlink, Remove.
func (a *App) Filecmd(r *sftp.Request) error {
	if a.sftConfig.ReadOnly {
		return sftp.ErrSSHFxPermissionDenied
	}
	switch r.Method {
	case "Mkdir":
	case "Remove", "Rmdir":
		err := a.deleteNeofsFile(r.Context(), r.Filepath)
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
	cnr, err := a.getContainerByName(r.Context(), split[0])
	if err != nil {
		return nil, err
	}

	obj := &ObjectInfo{
		FileName:  strings.TrimPrefix(trimmed, split[0]+delimiter),
		Container: cnr,
	}

	return newWriter(r.Context(), obj, a.pool, a.owner), nil
}

// Fileread prepares io.ReaderAt to download file.
// Called for Methods: Get.
func (a *App) Fileread(r *sftp.Request) (io.ReaderAt, error) {
	file, err := a.getFileStat(r.Context(), r.Filepath)
	if err != nil {
		return nil, err
	}

	obj, ok := file.(*ObjectInfo)
	if !ok {
		return nil, fmt.Errorf("couldn't get file stat")
	}

	return newReader(r.Context(), obj, a.pool), nil
}

// Filelist returns files information.
// Called for Methods: List, Stat, Readlink.
func (a *App) Filelist(r *sftp.Request) (sftp.ListerAt, error) {
	switch r.Method {
	case "List":
		files, err := a.listPath(r.Context(), r.Filepath)
		if err != nil {
			return nil, err
		}
		return ListerAt(files), nil
	case "Stat":
		stat, err := a.getFileStat(r.Context(), r.Filepath)
		if err != nil {
			return nil, err
		}
		return ListerAt([]os.FileInfo{stat}), nil
	case "Readlink":
	}

	return nil, errors.New("unsupported")
}

func newAddress(cnrID cid.ID, objID oid.ID) oid.Address {
	var addr oid.Address
	addr.SetContainer(cnrID)
	addr.SetObject(objID)
	return addr
}

func (w *objWriter) Close() error {
	attributes := make([]object.Attribute, 0, 2)
	filename := object.NewAttribute()
	filename.SetKey(object.AttributeFileName)
	filename.SetValue(w.file.Name())

	createdAt := object.NewAttribute()
	createdAt.SetKey(object.AttributeTimestamp)
	createdAt.SetValue(strconv.FormatInt(time.Now().UTC().Unix(), 10))

	attributes = append(attributes, *filename, *createdAt)

	obj := object.New()
	obj.SetOwnerID(w.owner)
	obj.SetContainerID(w.file.Container.CID)
	obj.SetAttributes(attributes...)

	var prm pool.PrmObjectPut
	prm.SetHeader(*obj)
	prm.SetPayload(w.buffer)

	_, err := w.pool.PutObject(w.ctx, prm)
	return err
}

func (w *objWriter) WriteAt(p []byte, off int64) (n int, err error) {
	if off != int64(w.buffer.Len()) {
		// todo consider support it or add regular put object streaming
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

	addr := newAddress(r.file.Container.CID, r.file.ObjectID)

	var prm pool.PrmObjectRange
	prm.SetAddress(addr)
	prm.SetOffset(uint64(off))
	prm.SetLength(length)

	res, err := r.pool.ObjectRange(r.ctx, prm)
	if err != nil {
		return 0, err
	}

	n, err = io.ReadFull(&res, b)
	if n < len(b) {
		err = io.EOF
	}
	return
}
