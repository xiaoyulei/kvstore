package etcdv3

import (
	"testing"
	"time"

	"github.com/kvstore"
	"github.com/kvstore/store"
	"github.com/kvstore/testutils"
	"github.com/stretchr/testify/assert"
)

var (
	client = "localhost:2379"
)

func makeEtcdClient(t *testing.T) store.Store {
	kv, err := New(
		[]string{client},
		&store.Config{
			ConnectionTimeout: 3 * time.Second,
			Username:          "test",
			Password:          "very-secure",
		},
	)

	if err != nil {
		t.Fatalf("cannot create store: %v", err)
	}

	return kv
}

func TestRegister(t *testing.T) {
	Register()

	kv, err := kvstore.NewStore(store.ETCD_V3, []string{client}, nil)
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	if _, ok := kv.(*Etcd); !ok {
		t.Fatal("Error registering and initializing etcd")
	}
}

func TestEtcdStore(t *testing.T) {
	kv := makeEtcdClient(t)
	lockKV := makeEtcdClient(t)
	ttlKV := makeEtcdClient(t)

	testutils.RunTestCommon(t, kv)
	testutils.RunTestAtomic(t, kv)
	testutils.RunTestWatch(t, kv)
	testutils.RunTestLockV3(t, kv)
	testutils.RunTestLockTTL(t, kv, lockKV)
	testutils.RunTestTTL(t, kv, ttlKV)
	testutils.RunCleanup(t, kv)
}
