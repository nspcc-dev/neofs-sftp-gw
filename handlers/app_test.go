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
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/pool"
	"github.com/nspcc-dev/neofs-sdk-go/user"
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

	var ownerID user.ID
	user.IDFromKey(&ownerID, key.PrivateKey.PublicKey)

	for _, version := range versions {
		ctx, cancel := context.WithCancel(rootCtx)

		aioContainer := createDockerContainer(ctx, t, version)

		clientPool := getPool(ctx, t, key)
		cnrID := createContainer(ctx, t, clientPool, ownerID)

		t.Run("test reader", func(t *testing.T) { testReader(ctx, t, clientPool, &ownerID, cnrID) })
		t.Run("test writer", func(t *testing.T) { testWriter(ctx, t, clientPool, &ownerID, cnrID) })

		err = aioContainer.Terminate(ctx)
		require.NoError(t, err)
		cancel()
	}
}

func testReader(ctx context.Context, t *testing.T, clientPool *pool.Pool, ownerID *user.ID, cnrID cid.ID) {
	content := "content for read test"
	id := putObject(ctx, t, clientPool, ownerID, cnrID, content, nil)

	obj := &ObjectInfo{
		Container: &ContainerInfo{
			CID: cnrID,
		},
		ObjectID:    id,
		PayloadSize: int64(len(content)),
	}

	reader := newReader(ctx, obj, clientPool)

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

func testWriter(ctx context.Context, t *testing.T, clientPool *pool.Pool, ownerID *user.ID, cnrID cid.ID) {
	content := "content for write test"

	obj := &ObjectInfo{
		Container: &ContainerInfo{
			CID: cnrID,
		},
		FileName: "write-test-object",
	}

	writer := newWriter(ctx, obj, clientPool, ownerID)

	_, err := writer.WriteAt(nil, -1)
	require.Error(t, err)

	b := []byte(content[:len(content)/2])
	n, err := writer.WriteAt(b, 0)
	require.NoError(t, err)

	b = []byte(content[n:])
	_, err = writer.WriteAt(b, int64(n))
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	payload, err := getObjectByName(ctx, clientPool, cnrID, obj.Name())
	require.NoError(t, err)

	require.Equal(t, content, string(payload))
}

func createDockerContainer(ctx context.Context, t *testing.T, image string) testcontainers.Container {
	req := testcontainers.ContainerRequest{
		Image:       image,
		WaitingFor:  wait.NewLogStrategy("aio container started").WithStartupTimeout(90 * time.Second),
		Name:        "sftp-gw-aio",
		Hostname:    "sftp-gw-aio",
		NetworkMode: "host",
	}
	aioC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	return aioC
}

func getPool(ctx context.Context, t *testing.T, key *keys.PrivateKey) *pool.Pool {
	var prm pool.InitParameters
	prm.SetKey(&key.PrivateKey)
	prm.SetNodeDialTimeout(5 * time.Second)
	prm.SetHealthcheckTimeout(5 * time.Second)
	prm.AddNode(pool.NewNodeParam(1, "localhost:8080", 1))

	clientPool, err := pool.NewPool(prm)
	require.NoError(t, err)

	err = clientPool.Dial(ctx)
	require.NoError(t, err)
	return clientPool
}

func createContainer(ctx context.Context, t *testing.T, clientPool *pool.Pool, ownerID user.ID) cid.ID {
	var policy netmap.PlacementPolicy
	err := policy.DecodeString("REP 1")
	require.NoError(t, err)

	var cnr container.Container
	cnr.Init()
	cnr.SetPlacementPolicy(policy)
	cnr.SetBasicACL(0x0FFFFFFF)
	cnr.SetOwner(ownerID)

	container.SetName(&cnr, "friendlyName")
	container.SetCreationTime(&cnr, time.Now())

	var wp pool.WaitParams
	wp.SetPollInterval(3 * time.Second)
	wp.SetTimeout(15 * time.Second)

	var prm pool.PrmContainerPut
	prm.SetContainer(cnr)

	cnrID, err := clientPool.PutContainer(ctx, prm)
	require.NoError(t, err)
	fmt.Println(cnrID.String())

	return cnrID
}

func putObject(ctx context.Context, t *testing.T, clientPool *pool.Pool, ownerID *user.ID, cnrID cid.ID, content string, attributes map[string]string) oid.ID {
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
	obj.SetPayload([]byte(content))

	var prm pool.PrmObjectPut
	prm.SetHeader(*obj)

	id, err := clientPool.PutObject(ctx, prm)
	require.NoError(t, err)

	return id
}

func getObjectByName(ctx context.Context, clientPool *pool.Pool, cnrID cid.ID, name string) ([]byte, error) {
	filter := object.NewSearchFilters()
	filter.AddRootFilter()
	filter.AddFilter(object.AttributeFileName, name, object.MatchStringEqual)

	var prm pool.PrmObjectSearch
	prm.SetContainerID(cnrID)
	prm.SetFilters(filter)

	res, err := clientPool.SearchObjects(ctx, prm)
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

	var prmGet pool.PrmObjectGet
	prmGet.SetAddress(newAddress(cnrID, ids[0]))

	resObj, err := clientPool.GetObject(ctx, prmGet)
	if err != nil {
		return nil, err
	}

	return io.ReadAll(resObj.Payload)
}
