package testutils

import (
	"fmt"
	"testing"
	"time"

	"github.com/kvstore/store"
	"github.com/stretchr/testify/assert"
)

// RunTestCommon tests the minimal required APIs which
// should be supported by all K/V backends
func RunTestCommon(t *testing.T, kv store.Store) {
	testPutGetDeleteExists(t, kv)
	testList(t, kv)
	testDeleteTree(t, kv)
}

// RunTestAtomic tests the Atomic operations by the K/V
// backends
func RunTestAtomic(t *testing.T, kv store.Store) {
	testAtomicPut(t, kv)
	testAtomicPutCreate(t, kv)
	testAtomicPutWithSlashSuffixKey(t, kv)
	testAtomicDelete(t, kv)
}

// RunTestWatch tests the watch/monitor APIs supported
// by the K/V backends.
func RunTestWatch(t *testing.T, kv store.Store) {
	testWatch(t, kv)
	testWatchTree(t, kv)
}

// RunTestLock tests the KV pair Lock/Unlock APIs supported
// by the K/V backends.
func RunTestLock(t *testing.T, kv store.Store) {
	testLockUnlock(t, kv)
}

// RunTestLockTTL tests the KV pair Lock with TTL APIs supported
// by the K/V backends.
func RunTestLockTTL(t *testing.T, kv store.Store, backup store.Store) {
	testLockTTL(t, kv, backup)
}

// RunTestTTL tests the TTL functionality of the K/V backend.
func RunTestTTL(t *testing.T, kv store.Store, backup store.Store) {
	testPutTTL(t, kv, backup)
}

func testPutGetDeleteExists(t *testing.T, kv store.Store) {
	// Get a not exist key should return ErrKeyNotFound
	pair, err := kv.Get("testPutGetDelete_not_exist_key")
	assert.Equal(t, store.ErrKeyNotFound, err)

	value := "bar"
	for _, key := range []string{
		"testPutGetDeleteExists",
		"testPutGetDeleteExists/",
		"testPutGetDeleteExists/testbar/",
		"testPutGetDeleteExists/testbar/testfoobar",
	} {
		failMsg := fmt.Sprintf("Fail key %s", key)

		// Put the key
		err = kv.Put(key, value, nil)
		assert.NoError(t, err, failMsg)

		// Get should return the value and an incremented index
		pair, err = kv.Get(key)
		assert.NoError(t, err, failMsg)
		if assert.NotNil(t, pair, failMsg) {
			assert.NotNil(t, pair.Value, failMsg)
		}
		assert.Equal(t, pair.Value, value, failMsg)

		// Exists should return true
		exists, err := kv.Exists(key)
		assert.NoError(t, err, failMsg)
		assert.True(t, exists, failMsg)

		// Delete the key
		err = kv.Delete(key)
		assert.NoError(t, err, failMsg)

		// Get should fail
		pair, err = kv.Get(key)
		assert.Error(t, err, failMsg)
		assert.Nil(t, pair, failMsg)

		// Exists should return false
		exists, err = kv.Exists(key)
		assert.NoError(t, err, failMsg)
		assert.False(t, exists, failMsg)
	}
}

func testWatch(t *testing.T, kv store.Store) {
	key := "testWatch"
	value := "world"
	newValue := "world!"

	// Put the key
	err := kv.Put(key, value, nil)
	assert.NoError(t, err)

	stopCh := make(chan struct{})
	defer func() {
		stopCh <- struct{}{}
	}()
	events, err := kv.Watch(key, stopCh)
	assert.NoError(t, err)
	assert.NotNil(t, events)

	// Update loop
	go func() {
		timeout := time.After(1 * time.Second)
		tick := time.Tick(250 * time.Millisecond)
		for {
			select {
			case <-timeout:
				return
			case <-tick:
				err := kv.Put(key, newValue, nil)
				if assert.NoError(t, err) {
					continue
				}
				return
			}
		}
	}()

	// Check for updates
	eventCount := 1
	for {
		select {
		case event := <-events:
			assert.NotNil(t, event)
			//assert.NotNil(t, event.PreNode)
			assert.NotNil(t, event.Node)

			if eventCount == 1 {
				assert.Equal(t, event.Action, store.ACTION_PUT)
				//assert.Equal(t, event.PreNode.Key, key)
				//assert.Equal(t, event.PreNode.Value, value)
				assert.Equal(t, event.Node.Key, key)
				assert.Equal(t, event.Node.Value, newValue)
			} else {
				//assert.Equal(t, event.PreNode.Key, key)
				//assert.Equal(t, event.PreNode.Value, newValue)
				assert.Equal(t, event.Node.Key, key)
				assert.Equal(t, event.Node.Value, newValue)
			}

			eventCount++
			// We received all the events we wanted to check
			if eventCount >= 4 {
				return
			}
		case <-time.After(4 * time.Second):
			t.Fatal("Timeout reached")
			return
		}
	}
}

func testWatchTree(t *testing.T, kv store.Store) {
	dir := "testWatchTree"

	node1 := "testWatchTree/node1"
	value1 := "node1"

	node2 := "testWatchTree/node2"
	value2 := "node2"

	node3 := "testWatchTree/node3"
	value3 := "node3"

	err := kv.Put(node1, value1, nil)
	assert.NoError(t, err)

	err = kv.Put(node2, value2, nil)
	assert.NoError(t, err)

	err = kv.Put(node3, value3, nil)
	assert.NoError(t, err)

	stopCh := make(chan struct{})
	defer close(stopCh)
	events, err := kv.WatchTree(dir, stopCh)
	assert.NoError(t, err)
	assert.NotNil(t, events)

	// Update loop
	go func() {
		timeout := time.After(500 * time.Millisecond)
		for {
			select {
			case <-timeout:
				err := kv.Delete(node3)
				assert.NoError(t, err)
				return
			}
		}
	}()

	// Check for updates
	for {
		select {
		case event := <-events:
			assert.NotNil(t, event)
			// We received the Delete event on a child node
			// Exit test successfully
			return
		case <-time.After(4 * time.Second):
			t.Fatal("Timeout reached")
			return
		}
	}
}

func testAtomicPut(t *testing.T, kv store.Store) {
	key := "testAtomicPut"
	value := "world"

	// Put the key
	err := kv.Put(key, value, nil)
	assert.NoError(t, err)

	// Get should return the value and an incremented index
	pair, err := kv.Get(key)
	assert.NoError(t, err)
	if assert.NotNil(t, pair) {
		assert.NotNil(t, pair.Value)
	}
	assert.Equal(t, pair.Value, value)

	// This CAS should fail: previous exists.
	err = kv.AtomicPut(key, "WORLD", nil, nil)
	assert.Error(t, err)

	// This CAS should succeed
	err = kv.AtomicPut(key, "WORLD", pair, nil)
	assert.NoError(t, err)

	// This CAS should fail, key exists.
	err = kv.AtomicPut(key, "WORLDWORLD", pair, nil)
	assert.Error(t, err)
}

func testAtomicPutCreate(t *testing.T, kv store.Store) {
	// Use a key in a new directory to ensure Stores will create directories
	// that don't yet exist.
	key := "testAtomicPutCreate/create"
	value := "putcreate"

	// AtomicPut the key, previous = nil indicates create.
	err := kv.AtomicPut(key, value, nil, nil)
	assert.NoError(t, err)

	// Get should return the value and an incremented index
	pair, err := kv.Get(key)
	assert.NoError(t, err)
	if assert.NotNil(t, pair) {
		assert.NotNil(t, pair.Value)
	}
	assert.Equal(t, pair.Value, value)

	// Attempting to create again should fail.
	err = kv.AtomicPut(key, value, nil, nil)
	assert.Error(t, store.ErrKeyExists)

	// This CAS should succeed, since it has the value from Get()
	err = kv.AtomicPut(key, "PUTCREATE", pair, nil)
	assert.NoError(t, err)
}

func testAtomicPutWithSlashSuffixKey(t *testing.T, kv store.Store) {
	k1 := "testAtomicPutWithSlashSuffixKey/key/"
	err := kv.AtomicPut(k1, "", nil, nil)
	assert.Nil(t, err)
}

func testAtomicDelete(t *testing.T, kv store.Store) {
	key := "testAtomicDelete"
	value := "world"

	// Put the key
	err := kv.Put(key, value, nil)
	assert.NoError(t, err)

	// Get should return the value and an incremented index
	pair, err := kv.Get(key)
	assert.NoError(t, err)
	if assert.NotNil(t, pair) {
		assert.NotNil(t, pair.Value)
	}
	assert.Equal(t, pair.Value, value)

	// AtomicDelete should succeed
	err = kv.AtomicDelete(key, pair)
	assert.NoError(t, err)

	// Delete a non-existent key; should fail
	err = kv.AtomicDelete(key, pair)
	assert.Error(t, store.ErrKeyNotFound)
}

func testLockUnlock(t *testing.T, kv store.Store) {
	key := "testLockUnlock"

	// We should be able to create a new lock on key
	lock := kv.NewLock(key, nil)
	assert.NotNil(t, lock)

	// Lock should successfully succeed or block
	lock.Lock()

	// Get should work
	pairs, err := kv.List(key)
	assert.NoError(t, err)
	if assert.NotNil(t, pairs) {
		if assert.Equal(t, len(pairs), 1) {
			assert.NotNil(t, pairs[0].Value)
		}
	}

	// Unlock should succeed
	lock.Unlock()

	// Lock should succeed again
	lock.Lock()

	// Get should work
	pairs, err = kv.List(key)
	assert.NoError(t, err)
	if assert.NotNil(t, pairs) {
		if assert.Equal(t, len(pairs), 1) {
			assert.NotNil(t, pairs[0].Value)
		}
	}

	lock.Unlock()
}

func testLockTTL(t *testing.T, kv store.Store, otherConn store.Store) {
	t.Skip("etcd v3 not support ttl")

	key := "testLockTTL"
	value := "bar"

	renewCh := make(chan struct{})

	// We should be able to create a new lock on key
	lock := otherConn.NewLock(key, &store.LockOptions{
		Value:     value,
		TTL:       2 * time.Second,
		RenewLock: renewCh,
	})

	assert.NotNil(t, lock)

	// Lock should successfully succeed
	lock.Lock()

	// Get should work
	pair, err := otherConn.Get(key)
	assert.NoError(t, err)
	if assert.NotNil(t, pair) {
		assert.NotNil(t, pair.Value)
	}
	assert.Equal(t, pair.Value, value)

	time.Sleep(3 * time.Second)

	done := make(chan struct{})
	stop := make(chan struct{})

	value = "foobar"

	// Create a new lock with another connection
	lock = kv.NewLock(
		key,
		&store.LockOptions{
			Value: value,
			TTL:   3 * time.Second,
		},
	)
	assert.NotNil(t, lock)

	// Lock should block, the session on the lock
	// is still active and renewed periodically
	go func(<-chan struct{}) {
		lock.Lock()
		done <- struct{}{}
	}(done)

	select {
	case _ = <-done:
		t.Fatal("Lock succeeded on a key that is supposed to be locked by another client")
	case <-time.After(4 * time.Second):
		// Stop requesting the lock as we are blocked as expected
		stop <- struct{}{}
		break
	}

	// Close the connection
	otherConn.Close()

	// Force stop the session renewal for the lock
	close(renewCh)

	// Let the session on the lock expire
	time.Sleep(3 * time.Second)
	locked := make(chan struct{})

	// Lock should now succeed for the other client
	go func(<-chan struct{}) {
		lock.Lock()
		locked <- struct{}{}
	}(locked)

	select {
	case _ = <-locked:
		break
	case <-time.After(4 * time.Second):
		t.Fatal("Unable to take the lock, timed out")
	}

	// Get should work with the new value
	pair, err = kv.Get(key)
	assert.NoError(t, err)
	if assert.NotNil(t, pair) {
		assert.NotNil(t, pair.Value)
	}
	assert.Equal(t, pair.Value, value)

	lock.Unlock()
}

func testPutTTL(t *testing.T, kv store.Store, otherConn store.Store) {
	firstKey := "testPutTTL"
	firstValue := "foo"

	secondKey := "second"
	secondValue := "bar"

	// Put the first key with the Ephemeral flag
	err := otherConn.Put(firstKey, firstValue, &store.WriteOptions{TTL: 2 * time.Second})
	assert.NoError(t, err)

	// Put a second key with the Ephemeral flag
	err = otherConn.Put(secondKey, secondValue, &store.WriteOptions{TTL: 2 * time.Second})
	assert.NoError(t, err)

	// Get on firstKey should work
	pair, err := kv.Get(firstKey)
	assert.NoError(t, err)
	assert.NotNil(t, pair)

	// Get on secondKey should work
	pair, err = kv.Get(secondKey)
	assert.NoError(t, err)
	assert.NotNil(t, pair)

	// Close the connection
	otherConn.Close()

	// Let the session expire
	time.Sleep(3 * time.Second)

	// Get on firstKey shouldn't work
	pair, err = kv.Get(firstKey)
	assert.Error(t, err)
	assert.Nil(t, pair)

	// Get on secondKey shouldn't work
	pair, err = kv.Get(secondKey)
	assert.Error(t, err)
	assert.Nil(t, pair)
}

func testList(t *testing.T, kv store.Store) {
	prefix := "testList"

	firstKey := "testList/first"
	firstValue := "first"

	secondKey := "testList/second"
	secondValue := "second"

	// Put the first key
	err := kv.Put(firstKey, firstValue, nil)
	assert.NoError(t, err)

	// Put the second key
	err = kv.Put(secondKey, secondValue, nil)
	assert.NoError(t, err)

	// List should work and return the two correct values
	for _, parent := range []string{prefix, prefix + "/"} {
		pairs, err := kv.List(parent)
		assert.NoError(t, err)
		if assert.NotNil(t, pairs) {
			assert.Equal(t, len(pairs), 2)
		}

		// Check pairs, those are not necessarily in Put order
		for _, pair := range pairs {
			if pair.Key == firstKey {
				assert.Equal(t, pair.Value, firstValue)
			}
			if pair.Key == secondKey {
				assert.Equal(t, pair.Value, secondValue)
			}
		}
	}

	// List should fail: the key does not exist
	pairs, err := kv.List("idontexist")
	assert.Equal(t, store.ErrKeyNotFound, err)
	assert.Nil(t, pairs)
}

func testDeleteTree(t *testing.T, kv store.Store) {
	prefix := "testDeleteTree"

	firstKey := "testDeleteTree/first"
	firstValue := "first"

	secondKey := "testDeleteTree/second"
	secondValue := "second"

	// Put the first key
	err := kv.Put(firstKey, firstValue, nil)
	assert.NoError(t, err)

	// Put the second key
	err = kv.Put(secondKey, secondValue, nil)
	assert.NoError(t, err)

	// Get should work on the first Key
	pair, err := kv.Get(firstKey)
	assert.NoError(t, err)
	if assert.NotNil(t, pair) {
		assert.NotNil(t, pair.Value)
	}
	assert.Equal(t, pair.Value, firstValue)

	// Get should work on the second Key
	pair, err = kv.Get(secondKey)
	assert.NoError(t, err)
	if assert.NotNil(t, pair) {
		assert.NotNil(t, pair.Value)
	}
	assert.Equal(t, pair.Value, secondValue)

	// Delete Values under directory `nodes`
	err = kv.DeleteTree(prefix)
	assert.NoError(t, err)

	// Get should fail on both keys
	pair, err = kv.Get(firstKey)
	assert.Error(t, err)
	assert.Nil(t, pair)

	pair, err = kv.Get(secondKey)
	assert.Error(t, err)
	assert.Nil(t, pair)
}

// RunCleanup cleans up keys introduced by the tests
func RunCleanup(t *testing.T, kv store.Store) {
	for _, key := range []string{
		"testAtomicPutWithSlashSuffixKey",
		"testPutGetDeleteExists",
		"testWatch",
		"testWatchTree",
		"testAtomicPut",
		"testAtomicPutCreate",
		"testAtomicDelete",
		"testLockUnlock",
		"testLockTTL",
		"testPutTTL",
		"testList",
		"testDeleteTree",
	} {
		err := kv.DeleteTree(key)
		assert.True(t, err == nil || err == store.ErrKeyNotFound, fmt.Sprintf("failed to delete tree key %s: %v", key, err))
		err = kv.Delete(key)
		assert.True(t, err == nil || err == store.ErrKeyNotFound, fmt.Sprintf("failed to delete key %s: %v", key, err))
	}
}
