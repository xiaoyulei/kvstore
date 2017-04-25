package etcdv3

import (
	"testing"
	"time"

	"github.com/YuleiXiao/kvstore/store"
	"github.com/YuleiXiao/kvstore/testutils"
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

func TestEtcdStore(t *testing.T) {
	kv := makeEtcdClient(t)
	lockKV := makeEtcdClient(t)
	ttlKV := makeEtcdClient(t)

	testutils.RunCleanup(t, kv)
	testutils.RunTestCommon(t, kv)
	testutils.RunTestAtomic(t, kv)
	testutils.RunTestWatch(t, kv)
	testutils.RunTestLockV3(t, kv)
	testutils.RunTestLockTTLV3(t, kv, lockKV)
	testutils.RunTestTTL(t, kv, ttlKV)
}
