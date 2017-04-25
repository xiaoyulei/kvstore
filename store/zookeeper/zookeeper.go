package zookeeper

import (
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/YuleiXiao/kvstore/store"
	zk "github.com/samuel/go-zookeeper/zk"
)

const (
	// SOH control character
	SOH = "\x01"

	defaultTimeout = 10 * time.Second
)

// Zookeeper is the receiver type for
// the Store interface
type Zookeeper struct {
	timeout     time.Duration
	client      *zk.Conn
	mu          sync.Mutex
	watchBuffer map[string]string
}

type zookeeperLock struct {
	client *zk.Conn
	lock   *zk.Lock
	key    string
	value  string
}

// New creates a new Zookeeper client given a
// list of endpoints and an optional tls config
func New(endpoints []string, options *store.Config) (store.Store, error) {
	s := &Zookeeper{
		watchBuffer: make(map[string]string),
		timeout:     defaultTimeout,
	}

	// Set options
	if options != nil {
		if options.ConnectionTimeout != 0 {
			s.setTimeout(options.ConnectionTimeout)
		}
	}

	// Connect to Zookeeper
	conn, _, err := zk.Connect(endpoints, s.timeout)
	if err != nil {
		return nil, err
	}
	s.client = conn

	return s, nil
}

// setTimeout sets the timeout for connecting to Zookeeper
func (s *Zookeeper) setTimeout(time time.Duration) {
	s.timeout = time
}

// Get the value at "key", returns the last modified index
// to use in conjunction to Atomic calls
func (s *Zookeeper) Get(ctx context.Context, key string) (pair *store.KVPair, err error) {
	fkey := store.Normalize(key)
	resp, meta, err := s.client.Get(fkey)
	if err != nil {
		if err == zk.ErrNoNode {
			return nil, store.ErrKeyNotFound
		}
		return nil, err
	}

	// FIXME handle very rare cases where Get returns the
	// SOH control character instead of the actual value
	if string(resp) == SOH {
		return s.Get(ctx, fkey)
	}

	pair = &store.KVPair{
		Key:   fkey,
		Value: string(resp),
		Index: uint64(meta.Version),
	}

	return pair, nil
}

// createFullPath creates the entire path for a directory
// that does not exist
func (s *Zookeeper) createFullPath(path []string, value string, ephemeral bool) error {
	for i := 1; i <= len(path); i++ {
		newpath := store.Normalize(strings.Join(path[:i], "/"))
		if i == len(path) {
			var flag int32
			if ephemeral {
				flag = zk.FlagEphemeral
			}
			_, err := s.client.Create(newpath, []byte(value), flag, zk.WorldACL(zk.PermAll))
			return err
		}

		_, err := s.client.Create(newpath, []byte{}, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			// Skip if node already exists
			if err != zk.ErrNodeExists {
				return err
			}
		}
	}
	return nil
}

// Put a value at "key"
func (s *Zookeeper) Put(ctx context.Context, key, value string, opts *store.WriteOptions) error {
	fkey := store.Normalize(key)
	exists, err := s.Exists(ctx, fkey)
	if err != nil {
		return err
	}

	if !exists {
		if opts != nil && opts.TTL > 0 {
			s.createFullPath(store.SplitKey(fkey), "", true)
		} else {
			s.createFullPath(store.SplitKey(fkey), "", false)
		}
	}

	_, err = s.client.Set(fkey, []byte(value), -1)
	return err
}

// Create is an alias for Put with key not exist
func (s *Zookeeper) Create(ctx context.Context, key, value string, opts *store.WriteOptions) error {
	fkey := store.Normalize(key)
	if opts != nil && opts.TTL > 0 {
		return s.createFullPath(store.SplitKey(fkey), value, true)
	}
	return s.createFullPath(store.SplitKey(fkey), value, false)
}

// Update is an alias for Put with key exist
func (s *Zookeeper) Update(ctx context.Context, key, value string, opts *store.WriteOptions) error {
	fkey := store.Normalize(key)
	exists, err := s.Exists(ctx, fkey)
	if err != nil {
		return err
	}
	if !exists {
		return store.ErrKeyNotFound
	}

	_, err = s.client.Set(fkey, []byte(value), -1)
	return err
}

// Delete a value at "key"
func (s *Zookeeper) Delete(ctx context.Context, key string) error {
	err := s.client.Delete(store.Normalize(key), -1)
	if err == zk.ErrNoNode {
		return store.ErrKeyNotFound
	}
	return err
}

// Exists checks if the key exists inside the store
func (s *Zookeeper) Exists(ctx context.Context, key string) (bool, error) {
	exists, _, err := s.client.Exists(store.Normalize(key))
	if err != nil {
		return false, err
	}
	return exists, nil
}

// Watch for changes on a "key"
// It returns a channel that will receive changes or pass
// on errors. Upon creation, the current value will first
// be sent to the channel. Providing a non-nil stopCh can
// be used to stop watching.
func (s *Zookeeper) Watch(ctx context.Context, key string, opt *store.WatchOptions) (<-chan *store.WatchResponse, error) {
	fkey := store.Normalize(key)

	// Catch zk notifications and fire changes into the channel.
	resp := make(chan *store.WatchResponse)
	go func() {
		defer close(resp)
		for {
			data, meta, eventCh, err := s.client.GetW(fkey)
			if err != nil {
				resp <- &store.WatchResponse{Error: err}
				return
			}

			select {
			case e := <-eventCh:
				if e.Type == zk.EventNodeDataChanged {
					if entry, err := s.Get(ctx, fkey); err == nil {
						resp <- &store.WatchResponse{
							Action: store.ActionPut,
							Node:   entry,
							PreNode: &store.KVPair{
								Key:   fkey,
								Value: string(data),
								Index: uint64(meta.Version),
							},
						}
					}
				} else if e.Type == zk.EventNodeDeleted {
					resp <- &store.WatchResponse{
						Action: store.ActionDelete,
						PreNode: &store.KVPair{
							Key:   fkey,
							Value: string(data),
							Index: uint64(meta.Version),
						},
					}
				}

			case <-ctx.Done():
				// There is no way to stop GetW so just quit
				resp <- &store.WatchResponse{Error: ctx.Err()}
				return
			}
		}
	}()

	return resp, nil
}

// WatchTree watches for changes on a "directory"
// It returns a channel that will receive changes or pass
// on errors. Upon creating a watch, the current childs values
// will be sent to the channel .Providing a non-nil stopCh can
// be used to stop watching.
func (s *Zookeeper) WatchTree(ctx context.Context, dir string, opt *store.WatchOptions) (<-chan *store.WatchResponse, error) {
	fkey := store.Normalize(dir)

	// Catch zk notifications and fire changes into the channel.
	resp := make(chan *store.WatchResponse)
	go func() {
		defer close(resp)
		for {
			data, meta, eventCh, err := s.client.ChildrenW(fkey)
			if err != nil {
				resp <- &store.WatchResponse{Error: err}
				return
			}

			select {
			case e := <-eventCh:
				if e.Type == zk.EventNodeChildrenChanged {
					pairs, err := s.List(ctx, fkey)
					respList := s.makeWatchResponse(data, meta, pairs, err)
					for _, r := range respList {
						resp <- r
					}
				}

			case <-ctx.Done():
				// There is no way to stop GetW so just quit
				resp <- &store.WatchResponse{Error: context.Canceled}
				return
			}
		}
	}()

	return resp, nil
}

// makeWatchResponse construct WatchResponse with nodes before and after event happen.
func (s *Zookeeper) makeWatchResponse(preNodes []string, stat *zk.Stat, paris []*store.KVPair, err error) []*store.WatchResponse {
	if err != nil {
		resp := &store.WatchResponse{Error: err}
		return []*store.WatchResponse{resp}
	}

	contain := func(s string, list []string) bool {
		for _, str := range list {
			if s == str {
				return true
			}
		}
		return false
	}

	var nodes []string
	respList := []*store.WatchResponse{}
	for _, kv := range paris {
		nodes = append(nodes, kv.Key)
		if !contain(kv.Key, preNodes) {
			respList = append(respList, &store.WatchResponse{
				Action: store.ActionPut,
				Node: &store.KVPair{
					Key:   kv.Key,
					Index: kv.Index,
					Value: kv.Value,
				},
			})
		}
	}

	for _, k := range preNodes {
		if !contain(k, nodes) {
			respList = append(respList, &store.WatchResponse{
				Action:  store.ActionDelete,
				PreNode: &store.KVPair{Key: k},
			})
		}
	}

	return respList
}

// List child nodes of a given directory
func (s *Zookeeper) List(ctx context.Context, directory string) ([]*store.KVPair, error) {
	fkey := store.Normalize(directory)
	keys, stat, err := s.client.Children(fkey)
	if err != nil {
		if err == zk.ErrNoNode {
			return nil, store.ErrKeyNotFound
		}
		return nil, err
	}

	kv := []*store.KVPair{}

	// FIXME Costly Get request for each child key..
	for _, key := range keys {
		pair, err := s.Get(ctx, strings.TrimSuffix(fkey, "/")+store.Normalize(key))
		if err != nil {
			// If node is not found: List is out of date, retry
			if err == store.ErrKeyNotFound {
				return s.List(ctx, fkey)
			}
			return nil, err
		}

		kv = append(kv, &store.KVPair{
			Key:   key,
			Value: pair.Value,
			Index: uint64(stat.Version),
		})
	}

	return kv, nil
}

// DeleteTree deletes a range of keys under a given directory
func (s *Zookeeper) DeleteTree(ctx context.Context, directory string) error {
	fkey := store.Normalize(directory)
	pairs, err := s.List(ctx, fkey)
	if err != nil {
		return err
	}

	var reqs []interface{}
	for _, pair := range pairs {
		reqs = append(reqs, &zk.DeleteRequest{
			Path:    store.Normalize(fkey + "/" + pair.Key),
			Version: -1,
		})
	}

	_, err = s.client.Multi(reqs...)
	return err
}

// AtomicPut put a value at "key" if the key has not been
// modified in the meantime, throws an error if this is the case
func (s *Zookeeper) AtomicPut(ctx context.Context, key, value string, previous *store.KVPair, _ *store.WriteOptions) error {
	fkey := store.Normalize(key)
	if previous != nil {
		_, err := s.client.Set(fkey, []byte(value), int32(previous.Index))
		if err != nil {
			// Compare Failed
			if err == zk.ErrBadVersion {
				return store.ErrKeyModified
			}
			return err
		}
	} else {
		// Interpret previous == nil as create operation.
		_, err := s.client.Create(fkey, []byte(value), 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			// Directory does not exist
			if err == zk.ErrNoNode {

				// Create the directory
				parts := store.SplitKey(fkey)
				parts = parts[:len(parts)-1]
				if err = s.createFullPath(parts, "", false); err != nil {
					// Failed to create the directory.
					return err
				}

				// Create the node
				if _, err := s.client.Create(fkey, []byte(value), 0, zk.WorldACL(zk.PermAll)); err != nil {
					// Node exist error (when previous nil)
					if err == zk.ErrNodeExists {
						return store.ErrKeyExists
					}
					return err
				}

			} else {
				// Node Exists error (when previous nil)
				if err == zk.ErrNodeExists {
					return store.ErrKeyExists
				}

				// Unhandled error
				return err
			}
		}
	}

	return nil
}

// AtomicDelete deletes a value at "key" if the key
// has not been modified in the meantime, throws an
// error if this is the case
func (s *Zookeeper) AtomicDelete(ctx context.Context, key string, previous *store.KVPair) error {
	if previous == nil {
		return store.ErrPreviousNotSpecified
	}

	err := s.client.Delete(store.Normalize(key), int32(previous.Index))
	if err != nil {
		// Key not found
		if err == zk.ErrNoNode {
			return store.ErrKeyNotFound
		}
		// Compare failed
		if err == zk.ErrBadVersion {
			return store.ErrKeyModified
		}
		// General store error
		return err
	}
	return nil
}

// NewLock returns a handle to a lock struct which can
// be used to provide mutual exclusion on a key
func (s *Zookeeper) NewLock(key string, options *store.LockOptions) store.Locker {
	fkey := store.Normalize(key)
	value := ""

	// Apply options
	if options != nil {
		value = options.Value
	}

	return &zookeeperLock{
		client: s.client,
		key:    fkey,
		value:  value,
		lock:   zk.NewLock(s.client, fkey, zk.WorldACL(zk.PermAll)),
	}
}

// Lock attempts to acquire the lock and blocks while
// doing so. It returns a channel that is closed if our
// lock is lost or if an error occurs
func (l *zookeeperLock) Lock(ctx context.Context) error {
	err := l.lock.Lock()
	if err == nil {
		// We hold the lock, we can set our value
		// FIXME: The value is left behind
		// (problematic for leader election)
		_, err = l.client.Set(l.key, []byte(l.value), -1)
	}
	return err
}

// Unlock the "key". Calling unlock while
// not holding the lock will throw an error
func (l *zookeeperLock) Unlock(ctx context.Context) error {
	return l.lock.Unlock()
}

// Close closes the client connection
func (s *Zookeeper) Close() {
	s.client.Close()
}
