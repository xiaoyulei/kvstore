package etcdv3

import (
	"log"
	"strings"

	"github.com/YuleiXiao/kvstore"
	"github.com/YuleiXiao/kvstore/store"
	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	mvccpb "github.com/coreos/etcd/mvcc/mvccpb"
)

// Etcd is the receiver type for the
// Store interface
type Etcd struct {
	client *etcd.Client
}

// Register registers etcd to kvstore
func Register() {
	kvstore.AddStore(store.ETCDV3, New)
}

// New creates a new Etcd client given a list
// of endpoints and an optional tls config
func New(addrs []string, options *store.Config) (store.Store, error) {
	cfg := &etcd.Config{
		Endpoints: addrs,
	}

	// Set options
	if options != nil {
		if options.TLS != nil {
			cfg.TLS = options.TLS
		}
		if options.ConnectionTimeout != 0 {
			cfg.DialTimeout = options.ConnectionTimeout
		}
		if options.Username != "" {
			cfg.Username = options.Username
			cfg.Password = options.Password
		}
	}

	c, err := etcd.New(*cfg)
	if err != nil {
		log.Fatal(err)
	}

	s := &Etcd{
		client: c,
	}

	return s, nil
}

// Normalize the key for usage in Etcd
func (s *Etcd) normalize(key string) string {
	key = store.Normalize(key)
	return strings.TrimPrefix(key, "/")
}

// Get the value at "key", returns the last modified
// index to use in conjunction to Atomic calls
func (s *Etcd) Get(key string) (pair *store.KVPair, err error) {
	pairs, err := s.get(key, false)
	if err != nil {
		return nil, err
	}

	return pairs[0], nil
}

func (s *Etcd) get(key string, prefix bool) (pairs []*store.KVPair, err error) {
	var resp *etcd.GetResponse
	var opts []etcd.OpOption
	if prefix {
		opts = []etcd.OpOption{etcd.WithPrefix()}
	}

	resp, err = s.client.Get(s.client.Ctx(), s.normalize(key), opts...)
	if err != nil {
		return nil, err
	}

	if resp.Count == 0 {
		return nil, store.ErrKeyNotFound
	}

	pairs = []*store.KVPair{}
	for _, kv := range resp.Kvs {
		pairs = append(pairs, &store.KVPair{
			Key:       string(kv.Key),
			Value:     string(kv.Value),
			LastIndex: uint64(kv.ModRevision),
		})
	}

	return pairs, nil
}

// Put a value at "key"
func (s *Etcd) Put(key, value string, opts *store.WriteOptions) error {
	if opts != nil {
		resp, err := s.client.Grant(s.client.Ctx(), int64(opts.TTL))
		if err != nil {
			log.Fatal(err)
		}
		_, err = s.client.Put(s.client.Ctx(), s.normalize(key), string(value), etcd.WithLease(resp.ID))
		return err
	}

	_, err := s.client.Put(s.client.Ctx(), s.normalize(key), string(value))
	return err
}

// Update is an alias for Put with key exist
func (s *Etcd) Update(key, value string, opts *store.WriteOptions) error {
	req := etcd.OpPut(key, value)
	if opts != nil {
		leaseResp, err := s.client.Grant(s.client.Ctx(), int64(opts.TTL))
		if err != nil {
			return err
		}

		req = etcd.OpPut(key, value, etcd.WithLease(leaseResp.ID))
	}

	txn := s.client.Txn(s.client.Ctx())
	resp, err := txn.If(etcd.Compare(etcd.CreateRevision(key), ">", 0)).Then(req).Commit()
	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return store.ErrKeyNotFound
	}

	return nil
}

// Create is an alias for Put with key not exist
func (s *Etcd) Create(key, value string, opts *store.WriteOptions) error {
	req := etcd.OpPut(key, value)
	if opts != nil {
		leaseResp, err := s.client.Grant(s.client.Ctx(), int64(opts.TTL))
		if err != nil {
			return err
		}

		req = etcd.OpPut(key, value, etcd.WithLease(leaseResp.ID))
	}

	txn := s.client.Txn(s.client.Ctx())
	resp, err := txn.If(etcd.Compare(etcd.CreateRevision(key), "=", 0)).Then(req).Commit()
	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return store.ErrKeyExists
	}

	return nil
}

// Delete a value at "key"
func (s *Etcd) Delete(key string) error {
	_, err := s.client.Delete(s.client.Ctx(), s.normalize(key))
	return err
}

// Exists checks if the key exists inside the store
func (s *Etcd) Exists(key string) (bool, error) {
	_, err := s.Get(key)
	if err != nil {
		if err == store.ErrKeyNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Watch for changes on a "key"
// It returns a channel that will receive changes or pass
// on errors. Upon creation, the current value will first
// be sent to the channel. Providing a non-nil stopCh can
// be used to stop watching.
func (s *Etcd) Watch(key string, opt *store.WatchOptions, stopCh <-chan struct{}) (<-chan *store.WatchResponse, error) {
	return s.watch(key, false, opt, stopCh)
}

// WatchTree watches for changes on a "directory"
// It returns a channel that will receive changes or pass
// on errors. Upon creating a watch, the current childs values
// will be sent to the channel. Providing a non-nil stopCh can
// be used to stop watching.
func (s *Etcd) WatchTree(directory string, opt *store.WatchOptions, stopCh <-chan struct{}) (<-chan *store.WatchResponse, error) {
	return s.watch(directory, true, opt, stopCh)
}

func (s *Etcd) watch(key string, prefix bool, opt *store.WatchOptions, stopCh <-chan struct{}) (<-chan *store.WatchResponse, error) {
	var watchChan etcd.WatchChan
	opts := []etcd.OpOption{etcd.WithPrevKV()}
	if prefix {
		opts = append(opts, etcd.WithPrefix())
	}
	if opt != nil {
		opts = append(opts, etcd.WithRev(int64(opt.Index)))
	}
	watchChan = s.client.Watch(s.client.Ctx(), s.normalize(key), opts...)

	// resp is sending back events to the caller
	resp := make(chan *store.WatchResponse)
	go func() {
		defer close(resp)
		for {
			select {
			case <-stopCh:
				return

			case ch := <-watchChan:
				resp <- s.makeWatchResponse(ch)
			}
		}
	}()

	return resp, nil
}

func (s *Etcd) makeWatchResponse(resp etcd.WatchResponse) *store.WatchResponse {
	for _, event := range resp.Events {
		switch event.Type {
		case mvccpb.PUT:
			var preNode *store.KVPair
			if event.PrevKv != nil {
				preNode = &store.KVPair{
					Key:       string(event.Kv.Key),
					Value:     string(event.Kv.Value),
					LastIndex: uint64(event.Kv.ModRevision),
				}
			}
			return &store.WatchResponse{
				Action:  store.ActionPut,
				PreNode: preNode,
				Node: &store.KVPair{
					Key:       string(event.Kv.Key),
					Value:     string(event.Kv.Value),
					LastIndex: uint64(event.Kv.ModRevision),
				},
			}

		case mvccpb.DELETE:
			return &store.WatchResponse{
				Action: store.ActionDelete,
				PreNode: &store.KVPair{
					Key:       string(event.Kv.Key),
					Value:     string(event.Kv.Value),
					LastIndex: uint64(event.Kv.ModRevision),
				},
				Node: nil,
			}
		default:
			log.Fatalf("Unexpected event type %v\n", event.Type)
			return nil
		}
	}
	return nil
}

// AtomicPut puts a value at "key" if the key has not been
// modified in the meantime, throws an error if this is the case
func (s *Etcd) AtomicPut(key, value string, previous *store.KVPair, opts *store.WriteOptions) error {
	req := etcd.OpPut(key, value)
	if opts != nil {
		leaseResp, err := s.client.Grant(s.client.Ctx(), int64(opts.TTL))
		if err != nil {
			return err
		}

		req = etcd.OpPut(key, value, etcd.WithLease(leaseResp.ID))
	}

	cmp := []etcd.Cmp{}
	if previous == nil {
		cmp = append(cmp, etcd.Compare(etcd.CreateRevision(key), "=", 0))
	} else {
		cmp = append(cmp, etcd.Compare(etcd.Value(key), "=", previous.Value))
		if previous.LastIndex != 0 {
			cmp = append(cmp, etcd.Compare(etcd.ModRevision(key), "=", int64(previous.LastIndex)))
		}
	}

	txn := s.client.Txn(s.client.Ctx())
	resp, err := txn.If(cmp...).Then(req).Commit()
	if err != nil {
		return err
	}

	if resp.Succeeded {
		return nil
	}

	if previous == nil {
		return store.ErrKeyExists
	}
	return store.ErrKeyModified
}

// AtomicDelete deletes a value at "key" if the key
// has not been modified in the meantime, throws an
// error if this is the case
func (s *Etcd) AtomicDelete(key string, previous *store.KVPair) error {
	if previous == nil {
		return store.ErrPreviousNotSpecified
	}

	cmp := []etcd.Cmp{etcd.Compare(etcd.Value(key), "=", previous.Value)}
	if previous.LastIndex != 0 {
		cmp = append(cmp, etcd.Compare(etcd.ModRevision(key), "=", int64(previous.LastIndex)))
	}

	txn := s.client.Txn(s.client.Ctx())
	resp, err := txn.If(cmp...).Then(
		etcd.OpDelete(key),
	).Commit()

	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return store.ErrKeyModified
	}

	return nil
}

// List child nodes of a given directory
func (s *Etcd) List(directory string) ([]*store.KVPair, error) {
	pairs, err := s.get(s.normalize(directory), true)
	if err != nil {
		return nil, err
	}

	return pairs, nil
}

// DeleteTree deletes a range of keys under a given directory
func (s *Etcd) DeleteTree(directory string) error {
	_, err := s.client.Delete(s.client.Ctx(), s.normalize(directory), etcd.WithPrefix())
	return err
}

// NewLock creates a lock for a given key.
// The returned Locker is not held and must be acquired
// with `.Lock`. The Value is optional.
// Now LockOptions not work in etcd v3.
func (s *Etcd) NewLock(key string, opt *store.LockOptions) store.Locker {
	//var session *concurrency.Session
	//if opt != nil {
	//	session, _ = concurrency.NewSession(s.client, concurrency.WithTTL(int(opt.TTL)))
	//} else {
	//	session, _ = concurrency.NewSession(s.client)
	//}
	//return concurrency.NewLocker(session, key)
	return concurrency.NewLocker(s.client, key)
}

// Close closes the client connection
func (s *Etcd) Close() {
	s.client.Close()
	return
}
