package store

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"time"

	"golang.org/x/net/context"
)

const (
	// ETCD backend
	ETCD = "etcd"

	// ETCDV3 backend
	ETCDV3 = "etcdv3"

	// ZK backend
	ZK = "zk"
)

var (
	// ErrBackendNotSupported is thrown when the backend k/v store is not supported by kvstore
	ErrBackendNotSupported = errors.New("Backend storage not supported yet, please choose one of")
	// ErrCallNotSupported is thrown when a method is not implemented/supported by the current backend
	ErrCallNotSupported = errors.New("The current call is not supported with this backend")
	// ErrNotReachable is thrown when the API cannot be reached for issuing common store operations
	ErrNotReachable = errors.New("Api not reachable")
	// ErrCannotLock is thrown when there is an error acquiring a lock on a key
	ErrCannotLock = errors.New("Error acquiring the lock")
	// ErrKeyModified is thrown during an atomic operation if the index does not match the one in the store
	ErrKeyModified = errors.New("Unable to complete atomic operation, key modified")
	// ErrKeyNotFound is thrown when the key is not found in the store during a Get operation
	ErrKeyNotFound = errors.New("Key not found in store")
	// ErrPreviousNotSpecified is thrown when the previous value is not specified for an atomic operation
	ErrPreviousNotSpecified = errors.New("Previous K/V pair should be provided for the Atomic operation")
	// ErrKeyExists is thrown when the previous value exists in the case of an AtomicPut
	ErrKeyExists = errors.New("Previous K/V pair exists, cannot complete Atomic operation")
	// ErrWatchFail is thrown when the watch fail or response channel closed
	ErrWatchFail = errors.New("Some error occurred when watch or response channel was closed")
)

// ActionXXX is the action definition of request.
const (
	ActionPut    = "PUT"
	ActionDelete = "DELETE"
)

// Config contains the options for a storage client
type Config struct {
	ClientTLS         *ClientTLSConfig
	TLS               *tls.Config
	ConnectionTimeout time.Duration
	Bucket            string
	PersistConnection bool
	Username          string
	Password          string
}

// ClientTLSConfig contains data for a Client TLS configuration in the form
// the etcd client wants it.  Eventually we'll adapt it for ZK and Consul.
type ClientTLSConfig struct {
	CertFile   string
	KeyFile    string
	CACertFile string
}

// Store represents the backend K/V storage
// Each store should support every call listed
// here. Or it couldn't be implemented as a K/V
// backend for kvstore
type Store interface {
	// Put a value at the specified key
	Put(ctx context.Context, key, value string, options *WriteOptions) error

	// Get a value given its key
	Get(ctx context.Context, key string) (*KVPair, error)

	// Delete the value at the specified key
	Delete(ctx context.Context, key string) error

	// Verify if a Key exists in the store
	Exists(ctx context.Context, key string) (bool, error)

	// Update is an alias for Put with key exist
	Update(ctx context.Context, key, value string, opts *WriteOptions) error

	// Create is an alias for Put with key not exist
	Create(ctx context.Context, key, value string, opts *WriteOptions) error

	// Watch for changes on a key
	Watch(ctx context.Context, key string, opt *WatchOptions) (<-chan *WatchResponse, error)

	// WatchTree watches for changes on child nodes under
	// a given directory
	WatchTree(ctx context.Context, directory string, opt *WatchOptions) (<-chan *WatchResponse, error)

	// NewLock creates a lock for a given key.
	// The returned Locker is not held and must be acquired
	// with `.Lock`. The Value is optional.
	NewLock(key string, opt *LockOptions) Locker

	// List the content of a given prefix
	List(ctx context.Context, directory string) ([]*KVPair, error)

	// DeleteTree deletes a range of keys under a given directory
	DeleteTree(ctx context.Context, directory string) error

	// Atomic CAS operation on a single value.
	// Pass previous = nil to create a new key.
	AtomicPut(ctx context.Context, key, value string, previous *KVPair, options *WriteOptions) error

	// Atomic delete of a single value
	AtomicDelete(ctx context.Context, key string, previous *KVPair) error

	// Compact compacts etcd KV history before the given rev.
	Compact(ctx context.Context, rev int64, wait bool) error

	// NewTxn creates a transaction Txn.
	NewTxn(ctx context.Context) (Txn, error)

	// Close the store connection
	Close()
}

// KVPair represents {Key, Value} tuple
type KVPair struct {
	Key   string
	Value string
	Index uint64
}

func (kv *KVPair) String() string {
	data, _ := json.Marshal(kv)
	return string(data)
}

// LockOptions contains optional request parameters
type LockOptions struct {
	Value     string        // Optional, value to associate with the lock
	TTL       time.Duration // Optional, expiration ttl associated with the lock
	RenewLock chan struct{} // Optional, chan used to control and stop the session ttl renewal for the lock
}

// Locker provides lock mechanism
type Locker interface {
	Lock(ctx context.Context) error
	Unlock(ctx context.Context) error
}

// WatchResponse will be returned when watch event happen.
type WatchResponse struct {
	Error   error
	Action  string
	PreNode *KVPair
	Node    *KVPair
}

func (wr *WatchResponse) String() string {
	data, _ := json.Marshal(wr)
	return string(data)
}

// WriteOptions contains optional request parameters
type WriteOptions struct {
	IsDir bool // useless in etcdv3
	TTL   time.Duration
}

// WatchOptions contains optional request parameters
type WatchOptions struct {
	Index uint64
}

// OpResponse will be returned when transaction commit.
type OpResponse struct {
	Pairs []*KVPair // fill on Get/List
}

// TxnResponse will be returned when transaction commit.
type TxnResponse struct {
	CompareSuccess bool
	Responses      []*OpResponse
}

func (t *TxnResponse) String() string {
	data, _ := json.Marshal(t)
	return string(data)
}

// Txn provides transaction interface.
type Txn interface {
	Begin()
	Commit() (*TxnResponse, error)

	// compare operation. Operator should only be "=" "!=" ">" "<".
	IfValue(key, operator, value string)
	IfCreateRevision(key, operator string, revision int64)
	IfModifyRevision(key, operator string, revision int64)

	// create operation
	Put(key, value string, options *WriteOptions)
	Get(key string)
	List(dir string)
	Delete(key string)
	DeleteTree(key string)

	// default operation execute when compare success.
	// If you want to execute when compare fail, you
	// should call "Else", all operation after "Else" will
	// execute when compare fail.
	Else()
}
