package etcd

import (
	"context"
	"crypto/tls"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/YuleiXiao/kvstore"
	"github.com/YuleiXiao/kvstore/store"
	etcd "github.com/coreos/etcd/client"
)

var (
	actionMap = map[string]string{
		"create":           store.ActionPut,
		"set":              store.ActionPut,
		"update":           store.ActionPut,
		"delete":           store.ActionPut,
		"compareAndSwap":   store.ActionPut,
		"compareAndDelete": store.ActionDelete,
		"expire":           store.ActionDelete,
	}
)

// Etcd is the receiver type for the
// Store interface
type Etcd struct {
	client etcd.KeysAPI
}

type etcdLock struct {
	client    etcd.KeysAPI
	stopLock  chan struct{}
	stopRenew chan struct{}
	key       string
	value     string
	last      *etcd.Response
	ttl       time.Duration
}

const (
	periodicSync      = 5 * time.Minute
	defaultLockTTL    = 20 * time.Second
	defaultUpdateTime = 5 * time.Second
)

// Register registers etcd to kvstore
func Register() {
	kvstore.AddStore(store.ETCD, New)
}

// New creates a new Etcd client given a list
// of endpoints and an optional tls config
func New(addrs []string, options *store.Config) (store.Store, error) {
	s := &Etcd{}

	var (
		entries []string
		err     error
	)

	entries = store.CreateEndpoints(addrs, "http")
	cfg := &etcd.Config{
		Endpoints:               entries,
		Transport:               etcd.DefaultTransport,
		HeaderTimeoutPerRequest: 3 * time.Second,
	}

	// Set options
	if options != nil {
		if options.TLS != nil {
			setTLS(cfg, options.TLS, addrs)
		}
		if options.ConnectionTimeout != 0 {
			setTimeout(cfg, options.ConnectionTimeout)
		}
		if options.Username != "" {
			setCredentials(cfg, options.Username, options.Password)
		}
	}

	c, err := etcd.New(*cfg)
	if err != nil {
		log.Fatal(err)
	}

	s.client = etcd.NewKeysAPI(c)

	// Periodic Cluster Sync
	go func() {
		for {
			if err := c.AutoSync(context.Background(), periodicSync); err != nil {
				return
			}
		}
	}()

	return s, nil
}

// SetTLS sets the tls configuration given a tls.Config scheme
func setTLS(cfg *etcd.Config, tls *tls.Config, addrs []string) {
	entries := store.CreateEndpoints(addrs, "https")
	cfg.Endpoints = entries

	// Set transport
	t := http.Transport{
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 10 * time.Second,
		TLSClientConfig:     tls,
	}

	cfg.Transport = &t
}

// setTimeout sets the timeout used for connecting to the store
func setTimeout(cfg *etcd.Config, time time.Duration) {
	cfg.HeaderTimeoutPerRequest = time
}

// setCredentials sets the username/password credentials for connecting to Etcd
func setCredentials(cfg *etcd.Config, username, password string) {
	cfg.Username = username
	cfg.Password = password
}

// keyNotFound checks on the error returned by the KeysAPI
// to verify if the key exists in the store or not
func keyNotFound(err error) bool {
	if err != nil {
		if etcdError, ok := err.(etcd.Error); ok {
			if etcdError.Code == etcd.ErrorCodeKeyNotFound ||
				etcdError.Code == etcd.ErrorCodeNotFile ||
				etcdError.Code == etcd.ErrorCodeNotDir {
				return true
			}
		}
	}
	return false
}

// Get the value at "key", returns the last modified
// index to use in conjunction to Atomic calls
func (s *Etcd) Get(key string) (pair *store.KVPair, err error) {
	key = store.Normalize(key)
	getOpts := &etcd.GetOptions{
		Quorum: true,
	}

	result, err := s.client.Get(context.Background(), key, getOpts)
	if err != nil {
		if keyNotFound(err) {
			return nil, store.ErrKeyNotFound
		}
		return nil, err
	}

	pair = &store.KVPair{
		Key:   key,
		Value: result.Node.Value,
		Index: result.Node.ModifiedIndex,
	}

	return pair, nil
}

// Put a value at "key"
func (s *Etcd) Put(key, value string, opts *store.WriteOptions) error {
	setOpts := &etcd.SetOptions{}

	// Set options
	if opts != nil {
		setOpts.Dir = opts.IsDir
		setOpts.TTL = opts.TTL
	}

	_, err := s.client.Set(context.Background(), store.Normalize(key), value, setOpts)
	return err
}

// Delete a value at "key"
func (s *Etcd) Delete(key string) error {
	opts := &etcd.DeleteOptions{
		Recursive: false,
	}

	_, err := s.client.Delete(context.Background(), store.Normalize(key), opts)
	if keyNotFound(err) {
		return store.ErrKeyNotFound
	}
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

// Update is an alias for Put with key exist
func (s *Etcd) Update(key, value string, opts *store.WriteOptions) error {
	setOpts := &etcd.SetOptions{PrevExist: etcd.PrevExist}

	// Set options
	if opts != nil {
		setOpts.TTL = opts.TTL
	}

	_, err := s.client.Set(context.Background(), store.Normalize(key), value, setOpts)
	return err
}

// Create is an alias for Put with key not exist
func (s *Etcd) Create(key, value string, opts *store.WriteOptions) error {
	setOpts := &etcd.SetOptions{PrevExist: etcd.PrevNoExist}

	// Set options
	if opts != nil {
		setOpts.TTL = opts.TTL
		setOpts.Dir = opts.IsDir
	}

	_, err := s.client.Set(context.Background(), store.Normalize(key), value, setOpts)
	return err
}

func (s *Etcd) makeWatchResponse(r *etcd.Response, err error) *store.WatchResponse {
	resp := &store.WatchResponse{Error: err}
	if err != nil {
		return resp
	}

	resp.Action = actionMap[r.Action]

	if r.PrevNode != nil {
		resp.PreNode = &store.KVPair{
			Key:   r.PrevNode.Key,
			Value: r.PrevNode.Value,
			Index: r.PrevNode.ModifiedIndex,
		}
	}

	if r.Node != nil {
		resp.Node = &store.KVPair{
			Key:   r.Node.Key,
			Value: r.Node.Value,
			Index: r.Node.ModifiedIndex,
		}
	}

	return resp
}

func (s *Etcd) watch(ctx context.Context, key string, opt *store.WatchOptions, recursive bool) (<-chan *store.WatchResponse, error) {
	opts := &etcd.WatcherOptions{Recursive: recursive}
	if opt != nil {
		opts.AfterIndex = opt.Index
	}
	watcher := s.client.Watcher(store.Normalize(key), opts)

	// resp is sending back events to the caller
	resp := make(chan *store.WatchResponse)
	go func() {
		defer close(resp)
		for {
			r, err := watcher.Next(ctx)
			resp <- s.makeWatchResponse(r, err)
			if err != nil {
				return
			}
		}
	}()

	return resp, nil
}

// Watch for changes on a "key"
// It returns a channel that will receive changes or pass
// on errors. Upon creation, the current value will first
// be sent to the channel. Providing a non-nil stopCh can
// be used to stop watching.
func (s *Etcd) Watch(ctx context.Context, key string, opt *store.WatchOptions) (<-chan *store.WatchResponse, error) {
	return s.watch(ctx, key, opt, false)
}

// WatchTree watches for changes on a "directory"
// It returns a channel that will receive changes or pass
// on errors. Upon creating a watch, the current childs values
// will be sent to the channel. Providing a non-nil stopCh can
// be used to stop watching.
func (s *Etcd) WatchTree(ctx context.Context, directory string, opt *store.WatchOptions) (<-chan *store.WatchResponse, error) {
	return s.watch(ctx, directory, opt, true)
}

// AtomicPut puts a value at "key" if the key has not been
// modified in the meantime, throws an error if this is the case
func (s *Etcd) AtomicPut(key, value string, previous *store.KVPair, opts *store.WriteOptions) error {
	var err error
	setOpts := &etcd.SetOptions{}

	if previous != nil {
		setOpts.PrevExist = etcd.PrevExist
		setOpts.PrevValue = previous.Value
		setOpts.PrevIndex = previous.Index
	} else {
		setOpts.PrevExist = etcd.PrevNoExist
	}

	if opts != nil {
		if opts.TTL > 0 {
			setOpts.TTL = opts.TTL
		}
	}

	_, err = s.client.Set(context.Background(), store.Normalize(key), value, setOpts)
	if err != nil {
		if etcdError, ok := err.(etcd.Error); ok {
			// Compare failed
			if etcdError.Code == etcd.ErrorCodeTestFailed {
				return store.ErrKeyModified
			}
			// Node exists error (when PrevNoExist)
			if etcdError.Code == etcd.ErrorCodeNodeExist {
				return store.ErrKeyExists
			}
		}
		return err
	}

	return nil
}

// AtomicDelete deletes a value at "key" if the key
// has not been modified in the meantime, throws an
// error if this is the case
func (s *Etcd) AtomicDelete(key string, previous *store.KVPair) error {
	if previous == nil {
		return store.ErrPreviousNotSpecified
	}

	delOpts := &etcd.DeleteOptions{}

	if previous != nil {
		delOpts.PrevValue = previous.Value
		delOpts.PrevIndex = previous.Index
	}

	_, err := s.client.Delete(context.Background(), store.Normalize(key), delOpts)
	if err != nil {
		if etcdError, ok := err.(etcd.Error); ok {
			// Key Not Found
			if etcdError.Code == etcd.ErrorCodeKeyNotFound {
				return store.ErrKeyNotFound
			}
			// Compare failed
			if etcdError.Code == etcd.ErrorCodeTestFailed {
				return store.ErrKeyModified
			}
		}
		return err
	}

	return nil
}

// List child nodes of a given directory
func (s *Etcd) List(directory string) ([]*store.KVPair, error) {
	getOpts := &etcd.GetOptions{
		Quorum:    true,
		Recursive: true,
		Sort:      true,
	}

	resp, err := s.client.Get(context.Background(), store.Normalize(directory), getOpts)
	if err != nil {
		if keyNotFound(err) {
			return nil, store.ErrKeyNotFound
		}
		return nil, err
	}

	kv := []*store.KVPair{}
	for _, n := range resp.Node.Nodes {
		kv = append(kv, &store.KVPair{
			Key:   n.Key,
			Value: n.Value,
			Index: n.ModifiedIndex,
		})
	}
	return kv, nil
}

// DeleteTree deletes a range of keys under a given directory
func (s *Etcd) DeleteTree(directory string) error {
	delOpts := &etcd.DeleteOptions{
		Recursive: true,
	}

	_, err := s.client.Delete(context.Background(), store.Normalize(directory), delOpts)
	if keyNotFound(err) {
		return store.ErrKeyNotFound
	}
	return err
}

// NewLock returns a handle to a lock struct which can
// be used to provide mutual exclusion on a key
func (s *Etcd) NewLock(key string, options *store.LockOptions) sync.Locker {
	var value string
	ttl := defaultLockTTL
	renewCh := make(chan struct{})

	// Apply options on Lock
	if options != nil {
		value = options.Value
		if options.TTL != 0 {
			ttl = options.TTL
		}
		if options.RenewLock != nil {
			renewCh = options.RenewLock
		}
	}

	// Create lock object
	return &etcdLock{
		client:    s.client,
		stopRenew: renewCh,
		key:       store.Normalize(key),
		value:     value,
		ttl:       ttl,
	}
}

// Lock attempts to acquire the lock and blocks while
// doing so. It returns a channel that is closed if our
// lock is lost or if an error occurs
func (l *etcdLock) Lock() {
	// Lock holder channel
	lockHeld := make(chan struct{})
	stopLocking := l.stopRenew

	setOpts := &etcd.SetOptions{
		TTL: l.ttl,
	}

	for {
		setOpts.PrevExist = etcd.PrevNoExist
		resp, err := l.client.Set(context.Background(), l.key, l.value, setOpts)
		if err != nil {
			if etcdError, ok := err.(etcd.Error); ok {
				if etcdError.Code != etcd.ErrorCodeNodeExist {
					log.Fatal(err)
				}
				setOpts.PrevIndex = ^uint64(0)
			}
		} else {
			setOpts.PrevIndex = resp.Node.ModifiedIndex
		}

		setOpts.PrevExist = etcd.PrevExist
		l.last, err = l.client.Set(context.Background(), l.key, l.value, setOpts)
		if err == nil {
			// Leader section
			l.stopLock = stopLocking
			go l.holdLock(l.key, lockHeld, stopLocking)
			break
		} else {
			// If this is a legitimate error, return
			if etcdError, ok := err.(etcd.Error); ok {
				if etcdError.Code != etcd.ErrorCodeTestFailed {
					log.Fatal(err)
				}
			}

			// Seeker section
			errorCh := make(chan error)
			chWStop := make(chan bool)
			free := make(chan bool)

			go l.waitLock(l.key, errorCh, chWStop, free)

			// Wait for the key to be available or for
			// a signal to stop trying to lock the key
			select {
			case <-free:
				break
			case err := <-errorCh:
				log.Fatal(err)
			}

			// Delete or Expire event occurred
			// Retry
		}
	}
}

// Hold the lock as long as we can
// Updates the key ttl periodically until we receive
// an explicit stop signal from the Unlock method
func (l *etcdLock) holdLock(key string, lockHeld chan struct{}, stopLocking <-chan struct{}) {
	defer close(lockHeld)

	update := time.NewTicker(l.ttl / 3)
	defer update.Stop()

	var err error
	setOpts := &etcd.SetOptions{TTL: l.ttl}

	for {
		select {
		case <-update.C:
			setOpts.PrevIndex = l.last.Node.ModifiedIndex
			l.last, err = l.client.Set(context.Background(), key, l.value, setOpts)
			if err != nil {
				return
			}

		case <-stopLocking:
			return
		}
	}
}

// WaitLock simply waits for the key to be available for creation
func (l *etcdLock) waitLock(key string, errorCh chan error, stopWatchCh chan bool, free chan<- bool) {
	opts := &etcd.WatcherOptions{Recursive: false}
	watcher := l.client.Watcher(key, opts)

	for {
		event, err := watcher.Next(context.Background())
		if err != nil {
			errorCh <- err
			return
		}
		if event.Action == "delete" || event.Action == "compareAndDelete" || event.Action == "expire" {
			free <- true
			return
		}
	}
}

// Unlock the "key". Calling unlock while
// not holding the lock will throw an error
func (l *etcdLock) Unlock() {
	if l.stopLock != nil {
		l.stopLock <- struct{}{}
	}
	if l.last != nil {
		delOpts := &etcd.DeleteOptions{
			PrevIndex: l.last.Node.ModifiedIndex,
		}
		_, err := l.client.Delete(context.Background(), l.key, delOpts)
		if err != nil {
			log.Fatal(err)
		}
	}
}

// Close closes the client connection
func (s *Etcd) Close() {
	return
}
