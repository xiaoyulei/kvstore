package kvstore

import (
	"fmt"
	"sort"
	"strings"

	"github.com/YuleiXiao/kvstore/store"
	"github.com/YuleiXiao/kvstore/store/etcd"
	"github.com/YuleiXiao/kvstore/store/etcdv3"
	"github.com/YuleiXiao/kvstore/store/zookeeper"
)

// Initialize creates a new Store object, initializing the client
type Initialize func(addrs []string, options *store.Config) (store.Store, error)

var (
	// Backend initializers
	initializers = make(map[string]Initialize)

	supportedBackend = func() string {
		keys := make([]string, 0, len(initializers))
		for k := range initializers {
			keys = append(keys, string(k))
		}
		sort.Strings(keys)
		return strings.Join(keys, ", ")
	}()
)

func init() {
	AddStore(store.ETCD, etcd.New)
	AddStore(store.ETCDV3, etcdv3.New)
	AddStore(store.ZK, zookeeper.New)
}

// NewStore creates an instance of store
func NewStore(backend string, addrs []string, options *store.Config) (store.Store, error) {
	if init, exists := initializers[backend]; exists {
		return init(addrs, options)
	}

	return nil, fmt.Errorf("%s %s", store.ErrBackendNotSupported.Error(), supportedBackend)
}

// AddStore adds a new store backend to kvstore
func AddStore(store string, init Initialize) {
	initializers[store] = init
}
