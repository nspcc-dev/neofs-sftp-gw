package handlers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/pool"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/nspcc-dev/neofs-sdk-go/waiter"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	versions = []string{
		"nspccdev/neofs-aio:0.38.1",
		"nspccdev/neofs-aio:0.39.0",
	}
)

func TestSftpHandlers(t *testing.T) {
	rootCtx := context.Background()
	key, err := keys.NewPrivateKeyFromHex("1dd37fba80fec4e6a6f13fd708d8dcb3b29def768017052f6c930fa1c5d90bbb")
	require.NoError(t, err)

	signer := user.NewAutoIDSignerRFC6979(key.PrivateKey)

	var ownerID = signer.UserID()

	for _, version := range versions {
		ctx, cancel := context.WithCancel(rootCtx)

		aioContainer, endpoint := createDockerContainer(ctx, t, version)

		clientPool := getPool(ctx, t, signer, endpoint)
		cnrID := createContainer(ctx, t, clientPool, ownerID, signer)

		t.Run("test reader", func(t *testing.T) { testReader(ctx, t, clientPool, &ownerID, cnrID, signer) })
		t.Run("test writer", func(t *testing.T) { testWriter(ctx, t, clientPool, &ownerID, cnrID, signer) })

		err = aioContainer.Terminate(ctx)
		require.NoError(t, err)
		cancel()
	}
}

func testReader(ctx context.Context, t *testing.T, clientPool *pool.Pool, ownerID *user.ID, cnrID cid.ID, signer user.Signer) {
	content := "content for read test"
	id := putObject(ctx, t, clientPool, ownerID, cnrID, content, nil, signer)

	obj := &ObjectInfo{
		Container: &ContainerInfo{
			CID: cnrID,
		},
		ObjectID:    id,
		PayloadSize: int64(len(content)),
	}

	reader := newReader(ctx, obj, clientPool, signer)

	_, err := reader.ReadAt(nil, -1)
	require.Error(t, err)
	_, err = reader.ReadAt(nil, int64(len(content)+1))
	require.Error(t, err)

	buff := bytes.NewBuffer(nil)

	b := make([]byte, len(content)/2)
	n, err := reader.ReadAt(b, 0)
	require.NoError(t, err)
	buff.Write(b[:n])

	b = make([]byte, len(content))
	n, err = reader.ReadAt(b, int64(n))
	require.Equal(t, io.EOF, err)
	buff.Write(b[:n])

	require.Equal(t, content, buff.String())
}

func testWriter(ctx context.Context, t *testing.T, clientPool *pool.Pool, ownerID *user.ID, cnrID cid.ID, signer user.Signer) {
	content := "content for write test"

	obj := &ObjectInfo{
		Container: &ContainerInfo{
			CID: cnrID,
		},
		FileName: "write-test-object",
	}

	ni, err := clientPool.NetworkInfo(ctx, client.PrmNetworkInfo{})
	require.NoError(t, err)

	writer, err := newWriter(ctx, obj, clientPool, ownerID, signer, ni.MaxObjectSize())
	require.NoError(t, err)

	_, err = writer.WriteAt(nil, -1)
	require.Error(t, err)

	b := []byte(content[:len(content)/2])
	n, err := writer.WriteAt(b, 0)
	require.NoError(t, err)

	b = []byte(content[n:])
	_, err = writer.WriteAt(b, int64(n))
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	payload, err := getObjectByName(ctx, clientPool, cnrID, obj.Name(), signer)
	require.NoError(t, err)

	require.Equal(t, content, string(payload))
}

func createDockerContainer(ctx context.Context, t *testing.T, image string) (testcontainers.Container, string) {
	req := testcontainers.ContainerRequest{
		Image:        image,
		WaitingFor:   wait.NewLogStrategy("aio container started").WithStartupTimeout(90 * time.Second),
		Name:         "sftp-gw-aio",
		Hostname:     "sftp-gw-aio",
		ExposedPorts: []string{"8080/tcp"},
	}
	aioC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	// for instance: localhost:32781
	nodeEndpoint, err := aioC.Endpoint(ctx, "")
	require.NoError(t, err)

	return aioC, nodeEndpoint
}

func getPool(ctx context.Context, t *testing.T, signer user.Signer, endpoint string) *pool.Pool {
	var prm pool.InitParameters
	prm.SetSigner(signer)
	prm.SetNodeDialTimeout(5 * time.Second)
	prm.SetHealthcheckTimeout(5 * time.Second)
	prm.AddNode(pool.NewNodeParam(1, endpoint, 1))

	clientPool, err := pool.NewPool(prm)
	require.NoError(t, err)

	err = clientPool.Dial(ctx)
	require.NoError(t, err)
	return clientPool
}

func createContainer(ctx context.Context, t *testing.T, clientPool *pool.Pool, ownerID user.ID, signer user.Signer) cid.ID {
	var policy netmap.PlacementPolicy
	err := policy.DecodeString("REP 1")
	require.NoError(t, err)

	var cnr container.Container
	cnr.Init()
	cnr.SetPlacementPolicy(policy)
	cnr.SetBasicACL(0x0FFFFFFF)
	cnr.SetOwner(ownerID)

	cnr.SetName("friendlyName")
	cnr.SetCreationTime(time.Now())

	var prm client.PrmContainerPut

	w := waiter.NewContainerPutWaiter(clientPool, 1*time.Second)

	cnrID, err := w.ContainerPut(ctx, cnr, signer, prm)
	require.NoError(t, err)
	fmt.Println(cnrID.String())

	return cnrID
}

func putObject(ctx context.Context, t *testing.T, clientPool *pool.Pool, ownerID *user.ID, cnrID cid.ID, content string, attributes map[string]string, signer user.Signer) oid.ID {
	obj := object.New()
	obj.SetContainerID(cnrID)
	obj.SetOwnerID(ownerID)

	var attrs []object.Attribute
	for key, val := range attributes {
		attr := object.NewAttribute()
		attr.SetKey(key)
		attr.SetValue(val)
		attrs = append(attrs, *attr)
	}
	obj.SetAttributes(attrs...)

	var prm client.PrmObjectPutInit

	ni, err := clientPool.NetworkInfo(ctx, client.PrmNetworkInfo{})
	require.NoError(t, err)

	chunk := make([]byte, ni.MaxObjectSize())

	writer, err := clientPool.ObjectPutInit(ctx, *obj, signer, prm)
	require.NoError(t, err)

	_, err = io.CopyBuffer(writer, bytes.NewBuffer([]byte(content)), chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	return writer.GetResult().StoredObjectID()
}

func getObjectByName(ctx context.Context, clientPool *pool.Pool, cnrID cid.ID, name string, signer user.Signer) ([]byte, error) {
	filter := object.NewSearchFilters()
	filter.AddRootFilter()
	filter.AddFilter(object.AttributeFileName, name, object.MatchStringEqual)

	var prm client.PrmObjectSearch
	prm.SetFilters(filter)

	res, err := clientPool.ObjectSearchInit(ctx, cnrID, signer, prm)
	if err != nil {
		return nil, fmt.Errorf("init searching using client: %w", err)
	}
	defer res.Close()

	ids := make([]oid.ID, 0, 2)

	err = res.Iterate(func(id oid.ID) bool {
		ids = append(ids, id)
		return len(ids) > 1
	})
	if err != nil {
		return nil, err
	}

	if len(ids) != 1 {
		return nil, errors.New("found not exactly one object")
	}

	var prmGet client.PrmObjectGet

	_, resObj, err := clientPool.ObjectGetInit(ctx, cnrID, ids[0], signer, prmGet)
	if err != nil {
		return nil, err
	}

	payload, err := io.ReadAll(resObj)
	if err != nil {
		return nil, err
	}

	if err = resObj.Close(); err != nil {
		return nil, err
	}

	return payload, nil
}
