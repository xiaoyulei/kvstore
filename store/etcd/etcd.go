package etcd

import (
	"crypto/tls"
	"errors"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"golang.org/x/net/context"

	etcd "github.com/coreos/etcd/client"
	"github.com/kvstore"
	"github.com/kvstore/store"
)

var (
	// ErrAbortTryLock is thrown when a user stops trying to seek the lock
	// by sending a signal to the stop chan, this is used to verify if the
	// operation succeeded
	ErrAbortTryLock = errors.New("lock operation aborted")

	actionMap = map[string]string{
		"create":           store.ACTION_PUT,
		"set":              store.ACTION_PUT,
		"update":           store.ACTION_PUT,
		"delete":           store.ACTION_DELETE,
		"compareAndSwap":   store.ACTION_PUT,
		"compareAndDelete": store.ACTION_DELETE,
		"expire":           store.ACTION_DELETE,
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

// Normalize the key for usage in Etcd
func (s *Etcd) normalize(key string) string {
	key = store.Normalize(key)
	return strings.TrimPrefix(key, "/")
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
	getOpts := &etcd.GetOptions{
		Quorum: true,
	}

	result, err := s.client.Get(context.Background(), s.normalize(key), getOpts)
	if err != nil {
		if keyNotFound(err) {
			return nil, store.ErrKeyNotFound
		}
		return nil, err
	}

	pair = &store.KVPair{
		Key:   key,
		Value: result.Node.Value,
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

	_, err := s.client.Set(context.Background(), s.normalize(key), string(value), setOpts)
	return err
}

// Delete a value at "key"
func (s *Etcd) Delete(key string) error {
	opts := &etcd.DeleteOptions{
		Recursive: false,
	}

	_, err := s.client.Delete(context.Background(), s.normalize(key), opts)
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

func (s *Etcd) makeWatchResponse(r *etcd.Response) *store.WatchResponse {
	var resp store.WatchResponse
	resp.Action = actionMap[r.Action]

	if r.PrevNode != nil {
		resp.PreNode = &store.KVPair{
			Key:   r.PrevNode.Key,
			Value: r.PrevNode.Value,
		}
	}

	if r.Node != nil {
		resp.Node = &store.KVPair{
			Key:   r.Node.Key,
			Value: r.Node.Value,
		}
	}

	return &resp
}

func (s *Etcd) watch(key string, stopCh <-chan struct{}, recursive bool) (<-chan *store.WatchResponse, error) {
	opts := &etcd.WatcherOptions{Recursive: recursive}
	watcher := s.client.Watcher(s.normalize(key), opts)

	// watchCh is sending back events to the caller
	resp := make(chan *store.WatchResponse)

	go func() {
		defer close(resp)

		for {
			// Check if the watch was stopped by the caller
			select {
			case <-stopCh:
				return
			default:
			}

			result, err := watcher.Next(context.Background())
			if err != nil {
				log.Fatalf("watcher next fail. %v", err)
			}

			resp <- s.makeWatchResponse(result)
		}
	}()

	return resp, nil
}

// Watch for changes on a "key"
// It returns a channel that will receive changes or pass
// on errors. Upon creation, the current value will first
// be sent to the channel. Providing a non-nil stopCh can
// be used to stop watching.
func (s *Etcd) Watch(key string, stopCh <-chan struct{}) (<-chan *store.WatchResponse, error) {
	return s.watch(key, stopCh, false)
}

// WatchTree watches for changes on a "directory"
// It returns a channel that will receive changes or pass
// on errors. Upon creating a watch, the current childs values
// will be sent to the channel. Providing a non-nil stopCh can
// be used to stop watching.
func (s *Etcd) WatchTree(directory string, stopCh <-chan struct{}) (<-chan *store.WatchResponse, error) {
	return s.watch(directory, stopCh, true)
}

// AtomicPut puts a value at "key" if the key has not been
// modified in the meantime, throws an error if this is the case
func (s *Etcd) AtomicPut(key, value string, previous *store.KVPair, opts *store.WriteOptions) error {
	var err error
	setOpts := &etcd.SetOptions{}

	if previous != nil {
		setOpts.PrevExist = etcd.PrevExist
		setOpts.PrevValue = previous.Value
	} else {
		setOpts.PrevExist = etcd.PrevNoExist
	}

	if opts != nil {
		if opts.TTL > 0 {
			setOpts.TTL = opts.TTL
		}
	}

	_, err = s.client.Set(context.Background(), s.normalize(key), string(value), setOpts)
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
	}

	_, err := s.client.Delete(context.Background(), s.normalize(key), delOpts)
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

	resp, err := s.client.Get(context.Background(), s.normalize(directory), getOpts)
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
		})
	}
	return kv, nil
}

// DeleteTree deletes a range of keys under a given directory
func (s *Etcd) DeleteTree(directory string) error {
	delOpts := &etcd.DeleteOptions{
		Recursive: true,
	}

	_, err := s.client.Delete(context.Background(), s.normalize(directory), delOpts)
	if keyNotFound(err) {
		return store.ErrKeyNotFound
	}
	return err
}

// NewLock returns a handle to a lock struct which can
// be used to provide mutual exclusion on a key
func (s *Etcd) NewLock(key string, options *store.LockOptions) (lock store.Locker) {
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
	lock = &etcdLock{
		client:    s.client,
		stopRenew: renewCh,
		key:       s.normalize(key),
		value:     value,
		ttl:       ttl,
	}

	return lock
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
					panic(err)
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
					panic(err)
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
				panic(err)
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
		if event.Action == "delete" || event.Action == "expire" {
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
			panic(err)
		}
	}
}

// Close closes the client connection
func (s *Etcd) Close() {
	return
}
