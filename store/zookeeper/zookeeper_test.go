package zookeeper

import (
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/YuleiXiao/kvstore"
	"github.com/YuleiXiao/kvstore/store"
	"github.com/YuleiXiao/kvstore/testutils"
	"github.com/stretchr/testify/assert"
)

var (
	client = "localhost:2181"
)

func makeZkClient(t *testing.T) store.Store {
	kv, err := New(
		[]string{client},
		&store.Config{
			ConnectionTimeout: 3 * time.Second,
		},
	)

	if err != nil {
		t.Fatalf("cannot create store: %v", err)
	}

	return kv
}

func TestRegister(t *testing.T) {
	Register()

	kv, err := kvstore.NewStore(store.ZK, []string{client}, nil)
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	if _, ok := kv.(*Zookeeper); !ok {
		t.Fatal("Error registering and initializing zookeeper")
	}
}

func testNewTxn(t *testing.T, kv store.Store) {
	_, err := kv.NewTxn(context.Background())
	if err != store.ErrCallNotSupported {
		t.Errorf("Txn should not be supported in etcdv2. %v", err)
	}
}

func TestZkStore(t *testing.T) {
	kv := makeZkClient(t)
	ttlKV := makeZkClient(t)

	testutils.RunCleanup(t, kv)
	testutils.RunTestCommon(t, kv)
	testutils.RunTestAtomic(t, kv)
	testutils.RunTestWatch(t, kv)
	testutils.RunTestLock(t, kv)
	testutils.RunTestTTL(t, kv, ttlKV)

	testNewTxn(t, kv)
}
