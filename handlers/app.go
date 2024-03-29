package handlers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/pool"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/nspcc-dev/neofs-sdk-go/waiter"
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

		pool                *pool.Pool
		owner               *user.ID
		signer              user.Signer
		sftConfig           *SftpServerConfig
		maxObjectSize       uint64
		defaultBucketPolicy string
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
		ctx    context.Context
		file   *ObjectInfo
		pool   *pool.Pool
		signer user.Signer
	}

	objWriter struct {
		ctx           context.Context
		file          *ObjectInfo
		pool          *pool.Pool
		owner         *user.ID
		signer        user.Signer
		buffer        *os.File
		maxObjectSize uint64
	}
)

// NewApp creates handlers (implements sftp.FileReader, sftp.FileWriter, sftp.FileCmder, sftp.FileLister).
func NewApp(conns *pool.Pool, signer user.Signer, owner *user.ID, l *zap.Logger, sftpConfig *SftpServerConfig,
	maxObjectSize uint64, defaultBucketPolicy string) *App {
	return &App{
		pool:                conns,
		signer:              signer,
		owner:               owner,
		Log:                 l,
		sftConfig:           sftpConfig,
		maxObjectSize:       maxObjectSize,
		defaultBucketPolicy: defaultBucketPolicy,
	}
}

func newReader(ctx context.Context, obj *ObjectInfo, conn *pool.Pool, signer user.Signer) *objReader {
	return &objReader{
		ctx:    ctx,
		file:   obj,
		pool:   conn,
		signer: signer,
	}
}

func newWriter(ctx context.Context, obj *ObjectInfo, conn *pool.Pool, ownerID *user.ID, signer user.Signer, maxObjectSize uint64) (*objWriter, error) {
	file, err := os.CreateTemp("", "sftpwriter")
	if err != nil {
		return nil, fmt.Errorf("CreateTemp: %w", err)
	}

	return &objWriter{
		ctx:           ctx,
		file:          obj,
		pool:          conn,
		owner:         ownerID,
		buffer:        file,
		signer:        signer,
		maxObjectSize: maxObjectSize,
	}, nil
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

	var prm client.PrmObjectSearch
	prm.SetFilters(filters)

	res, err := a.pool.ObjectSearchInit(ctx, cnrID, a.signer, prm)
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
	var prm client.PrmObjectHead
	objMeta, err := a.pool.ObjectHead(ctx, address.Container(), address.Object(), a.signer, prm)
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

	var prm client.PrmObjectSearch
	prm.SetFilters(filters)

	res, err := a.pool.ObjectSearchInit(ctx, cnrID, a.signer, prm)
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
	var prm client.PrmContainerGet
	cnr, err := a.pool.ContainerGet(ctx, cnrID, prm)
	if err != nil {
		return nil, err
	}

	file := &ContainerInfo{
		FileName: cnrID.EncodeToString(),
		CID:      cnrID,
		Created:  time.Now(),
	}

	if cnrName := cnr.Name(); len(cnrName) != 0 {
		file.FileName = cnrName
	}

	if createdTime := cnr.CreatedAt(); createdTime.IsZero() {
		file.Created = createdTime
	}

	return file, nil
}

func (a *App) listContainers(ctx context.Context) ([]os.FileInfo, error) {
	var result []os.FileInfo

	var prm client.PrmContainerList
	containers, err := a.pool.ContainerList(ctx, *a.owner, prm)
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

	var prm client.PrmContainerList
	containers, err := a.pool.ContainerList(ctx, *a.owner, prm)
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

		var prm client.PrmObjectDelete

		_, err = a.pool.ObjectDelete(ctx, cntr.CID, obj.ObjectID, a.signer, prm)
		return err
	}

	return a.deleteContainer(ctx, cntr.CID)
}

func (a *App) deleteContainer(ctx context.Context, cnrID cid.ID) error {
	var prm client.PrmContainerDelete
	return a.pool.ContainerDelete(ctx, cnrID, a.signer, prm)
}

// Filecmd called for Methods: Setstat, Rename, Rmdir, Mkdir, Link, Symlink, Remove.
func (a *App) Filecmd(r *sftp.Request) error {
	if a.sftConfig.ReadOnly {
		return sftp.ErrSSHFxPermissionDenied
	}
	switch r.Method {
	case "Mkdir":
		// valid Filepath "/somedir" or "somedir".
		path := strings.TrimPrefix(r.Filepath, delimiter)
		// invalid "/somedir/subdir", "somedir/subdir"
		if parts := strings.Split(path, delimiter); len(parts) > 1 {
			return fmt.Errorf("supported only first level dirs")
		}

		return a.putContainer(r.Context(), path, *a.owner, a.defaultBucketPolicy)
	case "Remove", "Rmdir":
		err := a.deleteNeofsFile(r.Context(), r.Filepath)
		return err
	}

	return nil
}

func (a *App) putContainer(ctx context.Context, name string, owner user.ID, policyStr string) error {
	var policy netmap.PlacementPolicy
	if err := policy.DecodeString(policyStr); err != nil {
		return fmt.Errorf("invalid placement policy: %w", err)
	}

	var cnr container.Container
	cnr.Init()
	cnr.SetPlacementPolicy(policy)
	cnr.SetBasicACL(acl.Private)
	cnr.SetOwner(owner)

	cnr.SetName(name)
	cnr.SetCreationTime(time.Now())

	var prm client.PrmContainerPut
	w := waiter.NewContainerPutWaiter(a.pool, waiter.DefaultPollInterval)

	if _, err := w.ContainerPut(ctx, cnr, a.signer, prm); err != nil {
		return fmt.Errorf("container put: %w", err)
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

	w, err := newWriter(r.Context(), obj, a.pool, a.owner, a.signer, a.maxObjectSize)
	if err != nil {
		return nil, fmt.Errorf("newWriter: %w", err)
	}

	return w, nil
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

	return newReader(r.Context(), obj, a.pool, a.signer), nil
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
	defer func() {
		if err := os.Remove(w.buffer.Name()); err != nil {
			zap.L().Error("remove tmp file", zap.String("file", w.buffer.Name()), zap.Error(err))
		}
	}()

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

	var prm client.PrmObjectPutInit

	writer, err := w.pool.ObjectPutInit(w.ctx, *obj, w.signer, prm)
	if err != nil {
		return fmt.Errorf("ObjectPutInit: %w", err)
	}

	chunk := make([]byte, w.maxObjectSize)
	if _, err = io.CopyBuffer(writer, w.buffer, chunk); err != nil {
		return fmt.Errorf("CopyBuffer: %w", err)
	}

	if err = writer.Close(); err != nil {
		return fmt.Errorf("writer close: %w", err)
	}

	return err
}

func (w *objWriter) WriteAt(p []byte, off int64) (n int, err error) {
	return w.buffer.WriteAt(p, off)
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

	var prm client.PrmObjectRange

	res, err := r.pool.ObjectRangeInit(r.ctx, addr.Container(), addr.Object(), uint64(off), length, r.signer, prm)
	if err != nil {
		return 0, err
	}

	n, err = io.ReadFull(res, b)
	if n < len(b) {
		err = io.EOF
	}
	return
}
