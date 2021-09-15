package handlers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	cid "github.com/nspcc-dev/neofs-api-go/pkg/container/id"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-sdk-go/policy"
	"github.com/nspcc-dev/neofs-sdk-go/pool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const testImage = "nspccdev/neofs-aio-testcontainer:0.24.0"

func TestSftpHandlers(t *testing.T) {
	key, err := keys.NewPrivateKeyFromHex("1dd37fba80fec4e6a6f13fd708d8dcb3b29def768017052f6c930fa1c5d90bbb")
	require.NoError(t, err)

	ctx := context.Background()
	ctnr := createDockerContainer(ctx, t, testImage)
	defer func() {
		_ = ctnr.Terminate(ctx)
	}()

	clientPool := getPool(ctx, t, key)
	CID := createContainer(ctx, t, clientPool)

	t.Run("test reader", func(t *testing.T) { testReader(ctx, t, clientPool, CID) })
	t.Run("test writer", func(t *testing.T) { testWriter(ctx, t, clientPool, CID) })
}

func testReader(ctx context.Context, t *testing.T, clientPool pool.Pool, CID *cid.ID) {
	content := "content for read test"
	oid := putObject(ctx, t, clientPool, CID, content, nil)

	obj := &ObjectInfo{
		Container: &ContainerInfo{
			CID: CID,
		},
		OID:         oid,
		PayloadSize: int64(len(content)),
	}

	reader := newReader(obj, clientPool)

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

func testWriter(ctx context.Context, t *testing.T, clientPool pool.Pool, CID *cid.ID) {
	content := "content for write test"

	obj := &ObjectInfo{
		Container: &ContainerInfo{
			CID: CID,
		},
		FileName: "write-test-object",
	}

	writer := newWriter(obj, clientPool)

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

	payload, err := getObjectByName(ctx, clientPool, CID, obj.Name())
	require.NoError(t, err)

	require.Equal(t, content, string(payload))
}

func createDockerContainer(ctx context.Context, t *testing.T, image string) testcontainers.Container {
	req := testcontainers.ContainerRequest{
		Image:       image,
		WaitingFor:  wait.NewLogStrategy("aio container started").WithStartupTimeout(30 * time.Second),
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

func getPool(ctx context.Context, t *testing.T, key *keys.PrivateKey) pool.Pool {
	pb := new(pool.Builder)
	pb.AddNode("localhost:8080", 1)

	opts := &pool.BuilderOptions{
		Key:                   &key.PrivateKey,
		NodeConnectionTimeout: 5 * time.Second,
		NodeRequestTimeout:    5 * time.Second,
	}
	clientPool, err := pb.Build(ctx, opts)
	require.NoError(t, err)
	return clientPool
}

func createContainer(ctx context.Context, t *testing.T, clientPool pool.Pool) *cid.ID {
	pp, err := policy.Parse("REP 1")
	require.NoError(t, err)

	cnr := container.New(
		container.WithPolicy(pp),
		container.WithCustomBasicACL(0x0FFFFFFF),
		container.WithAttribute(container.AttributeName, "friendlyName"),
		container.WithAttribute(container.AttributeTimestamp, strconv.FormatInt(time.Now().Unix(), 10)))
	cnr.SetOwnerID(clientPool.OwnerID())

	CID, err := clientPool.PutContainer(ctx, cnr)
	require.NoError(t, err)
	fmt.Println(CID.String())

	err = clientPool.WaitForContainerPresence(ctx, CID, &pool.ContainerPollingParams{
		CreationTimeout: 15 * time.Second,
		PollInterval:    3 * time.Second,
	})
	require.NoError(t, err)

	return CID
}

func putObject(ctx context.Context, t *testing.T, clientPool pool.Pool, CID *cid.ID, content string, attributes map[string]string) *object.ID {
	rawObject := object.NewRaw()
	rawObject.SetContainerID(CID)
	rawObject.SetOwnerID(clientPool.OwnerID())

	var attrs []*object.Attribute
	for key, val := range attributes {
		attr := object.NewAttribute()
		attr.SetKey(key)
		attr.SetValue(val)
		attrs = append(attrs, attr)
	}
	rawObject.SetAttributes(attrs...)

	ops := new(client.PutObjectParams).WithObject(rawObject.Object()).WithPayloadReader(bytes.NewBufferString(content))
	oid, err := clientPool.PutObject(ctx, ops)
	require.NoError(t, err)

	return oid
}

func getObjectByName(ctx context.Context, clientPool pool.Pool, CID *cid.ID, name string) ([]byte, error) {
	filter := object.NewSearchFilters()
	filter.AddRootFilter()
	filter.AddFilter(object.AttributeFileName, name, object.MatchStringEqual)

	params := new(client.SearchObjectParams).WithContainerID(CID).WithSearchFilters(filter)

	ids, err := clientPool.SearchObject(ctx, params)
	if err != nil {
		return nil, err
	}
	if len(ids) != 1 {
		return nil, errors.New("found not exactly one object")
	}

	return getObject(ctx, clientPool, newAddress(CID, ids[0]))
}

func getObject(ctx context.Context, clientPool pool.Pool, address *object.Address) ([]byte, error) {
	payload := bytes.NewBuffer(nil)
	ops := new(client.GetObjectParams).WithAddress(address).WithPayloadWriter(payload)
	_, err := clientPool.GetObject(ctx, ops)
	return payload.Bytes(), err
}
