package kvstore

import (
	"fmt"
	"testing"
	"time"

	"github.com/YuleiXiao/kvstore/store"
	"github.com/stretchr/testify/assert"
)

func TestNewStoreUnsupported(t *testing.T) {
	client := "localhost:9999"

	kv, err := NewStore(
		"unsupported",
		[]string{client},
		&store.Config{
			ConnectionTimeout: 10 * time.Second,
		},
	)
	assert.Error(t, err)
	assert.Nil(t, kv)
	assert.Equal(t, "Backend storage not supported yet, please choose one of ", err.Error())
}

func TestAddStore(t *testing.T) {
	testFun := func(addrs []string, options *store.Config) (store.Store, error) {
		return nil, fmt.Errorf("testFun")
	}

	AddStore("test", testFun)
	if _, ok := initializers["test"]; !ok {
		assert.Equal(t, true, ok)
	}
}
