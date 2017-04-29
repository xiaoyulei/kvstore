package testutils

import (
	"fmt"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/YuleiXiao/kvstore/store"
	"github.com/stretchr/testify/assert"
)

// RunTestCommon tests the minimal required APIs which
// should be supported by all K/V backends
func RunTestCommon(t *testing.T, kv store.Store) {
	testPutGetDeleteExistsUpdateCreate(t, kv)
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

// RunTestLockV3 tests the KV pair Lock/Unlock APIs supported
// by etcd client v3.
func RunTestLockV3(t *testing.T, kv store.Store) {
	testLockUnlockV3(t, kv)
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

// RunTestLockTTLV3 tests the KV pair Lock with TTL APIs supported
// by the K/V backends.
func RunTestLockTTLV3(t *testing.T, kv store.Store, backup store.Store) {
	testLockTTLV3(t, kv, backup)
}

// RunTestTTL tests the TTL functionality of the K/V backend.
func RunTestTTL(t *testing.T, kv store.Store, backup store.Store) {
	testPutTTL(t, kv, backup)
}

func testPutGetDeleteExistsUpdateCreate(t *testing.T, kv store.Store) {
	// Get a not exist key should return ErrKeyNotFound
	pair, err := kv.Get(context.TODO(), "testPutGetDeleteUpdateCreate_not_exist_key")
	assert.Equal(t, store.ErrKeyNotFound, err)

	value := "bar"
	for _, key := range []string{
		"testPutGetDeleteExistsUpdateCreate",
		"testPutGetDeleteExistsUpdateCreate/",
		"testPutGetDeleteExistsUpdateCreate/testbar/",
		"testPutGetDeleteExistsUpdateCreate/testbar/testfoobar",
	} {
		failMsg := fmt.Sprintf("Fail key %s", key)

		// Update no exist key
		err = kv.Update(context.TODO(), key, value, nil)
		assert.Error(t, err, failMsg)

		// Create no exist key
		err = kv.Create(context.TODO(), key, value, nil)
		assert.NoError(t, err, failMsg)

		// Put the key
		err = kv.Put(context.TODO(), key, value, nil)
		assert.NoError(t, err, failMsg)

		// Get should return the value and an incremented index
		pair, err = kv.Get(context.TODO(), key)
		assert.NoError(t, err, failMsg)
		if assert.NotNil(t, pair, failMsg) {
			assert.NotNil(t, pair.Value, failMsg)
		}
		assert.Equal(t, pair.Value, value, failMsg)
		assert.NotEqual(t, pair.Index, 0, failMsg)

		// Exists should return true
		exists, err := kv.Exists(context.TODO(), key)
		assert.NoError(t, err, failMsg)
		assert.True(t, exists, failMsg)

		// Update exist key
		err = kv.Update(context.TODO(), key, value, nil)
		assert.NoError(t, err, failMsg)

		// Create exist key
		err = kv.Create(context.TODO(), key, value, nil)
		assert.Error(t, err, failMsg)

		// Delete the key
		err = kv.Delete(context.TODO(), key)
		assert.NoError(t, err, failMsg)

		// Get should fail
		pair, err = kv.Get(context.TODO(), key)
		assert.Error(t, err, failMsg)
		assert.Nil(t, pair, failMsg)

		// Exists should return false
		exists, err = kv.Exists(context.TODO(), key)
		assert.NoError(t, err, failMsg)
		assert.False(t, exists, failMsg)
	}
}

func testWatch(t *testing.T, kv store.Store) {
	key := "/testWatch"
	key1 := "/testWatch_1"

	value := "world"
	newValue := "world!"

	// Put the key
	err := kv.Put(context.TODO(), key, value, nil)
	assert.NoError(t, err)

	ctx, cancle := context.WithCancel(context.Background())
	defer func() {
		cancle()
	}()
	events, err := kv.Watch(ctx, key, nil)
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
				err := kv.Put(context.TODO(), key, newValue, nil)
				if assert.NoError(t, err) {
					continue
				}
				return
			}
		}
	}()

	// Check for updates
	eventCount := 1

LOOP:
	for {
		select {
		case event := <-events:
			assert.NotNil(t, event)
			assert.NotNil(t, event.PreNode)
			assert.NotNil(t, event.Node)

			if eventCount == 1 {
				assert.Equal(t, event.Action, store.ActionPut)
				assert.Equal(t, event.PreNode.Key, key)
				assert.Equal(t, event.PreNode.Value, value)
				assert.Equal(t, event.Node.Key, key)
				assert.Equal(t, event.Node.Value, newValue)
			} else {
				assert.Equal(t, event.PreNode.Key, key)
				assert.Equal(t, event.PreNode.Value, newValue)
				assert.Equal(t, event.Node.Key, key)
				assert.Equal(t, event.Node.Value, newValue)
			}

			eventCount++
			// We received all the events we wanted to check
			if eventCount >= 4 {
				break LOOP
			}
		case <-time.After(4 * time.Second):
			break LOOP
		}
	}

	failMsg := "stop watch fail"

	// stop watch by context
	ctx1, cancle1 := context.WithCancel(context.Background())
	events1, err := kv.Watch(ctx1, key1, nil)
	assert.NoError(t, err)
	assert.NotNil(t, events1)
	cancle1()

	e, ok := <-events1
	assert.NotNil(t, e)
	assert.Equal(t, true, ok, failMsg)
	assert.Error(t, e.Error, failMsg)

	e, ok = <-events1
	assert.Nil(t, e)
	assert.Equal(t, false, ok, failMsg)

	// put a key, watch and delete, check delete event
	ctx2, _ := context.WithCancel(context.Background())
	err = kv.Put(ctx2, key, value, nil)
	assert.NoError(t, err)

	events2, err := kv.Watch(ctx2, key, nil)
	fmt.Println("========================================================")

	assert.NoError(t, err)
	assert.NotNil(t, events2)
	err = kv.Delete(ctx2, key)
	assert.NoError(t, err)

	time.Sleep(3 * time.Second)
	for e := range events2 {
		fmt.Printf("--------%#v\n", e)
	}
	//assert.NotNil(t, e)
	//assert.Equal(t, true, ok, failMsg)
	//assert.NotNil(t, e.Node)
	//assert.NotNil(t, e.PreNode)
	//fmt.Println("========================================================")
	//
	//cancle2()
	//fmt.Println("========================================================")

}

func testWatchTree(t *testing.T, kv store.Store) {
	dir := "testWatchTree"

	node1 := "testWatchTree/node1"
	value1 := "node1"

	node2 := "testWatchTree/node2"
	value2 := "node2"

	node3 := "testWatchTree/node3"
	value3 := "node3"

	err := kv.Put(context.TODO(), node1, value1, nil)
	assert.NoError(t, err)

	err = kv.Put(context.TODO(), node2, value2, nil)
	assert.NoError(t, err)

	err = kv.Put(context.TODO(), node3, value3, nil)
	assert.NoError(t, err)

	ctx, cancle := context.WithCancel(context.Background())
	defer cancle()
	events, err := kv.WatchTree(ctx, dir, nil)
	assert.NoError(t, err)
	assert.NotNil(t, events)

	// Update loop
	go func() {
		timeout := time.After(500 * time.Millisecond)
		for {
			select {
			case <-timeout:
				err := kv.Delete(context.TODO(), node3)
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
	err := kv.Put(context.TODO(), key, value, nil)
	assert.NoError(t, err)

	// Get should return the value and an incremented index
	pair, err := kv.Get(context.TODO(), key)
	assert.NoError(t, err)
	if assert.NotNil(t, pair) {
		assert.NotNil(t, pair.Value)
	}
	assert.Equal(t, pair.Value, value)
	assert.NotEqual(t, pair.Index, 0)

	// This CAS should fail: previous exists.
	err = kv.AtomicPut(context.TODO(), key, "WORLD", nil, nil)
	assert.Error(t, err)

	// This CAS should succeed
	err = kv.AtomicPut(context.TODO(), key, "WORLD", pair, nil)
	assert.NoError(t, err)

	// This CAS should fail, key exists.
	pair.Index = 6744
	err = kv.AtomicPut(context.TODO(), key, "WORLDWORLD", pair, nil)
	assert.Error(t, err)
}

func testAtomicPutCreate(t *testing.T, kv store.Store) {
	// Use a key in a new directory to ensure Stores will create directories
	// that don't yet exist.
	key := "testAtomicPutCreate/create"
	value := "putcreate"

	// AtomicPut the key, previous = nil indicates create.
	err := kv.AtomicPut(context.TODO(), key, value, nil, nil)
	assert.NoError(t, err)

	// Get should return the value and an incremented index
	pair, err := kv.Get(context.TODO(), key)
	assert.NoError(t, err)
	if assert.NotNil(t, pair) {
		assert.NotNil(t, pair.Value)
	}
	assert.Equal(t, pair.Value, value)

	// Attempting to create again should fail.
	err = kv.AtomicPut(context.TODO(), key, value, nil, nil)
	assert.Error(t, store.ErrKeyExists)

	// This CAS should succeed, since it has the value from Get()
	err = kv.AtomicPut(context.TODO(), key, "PUTCREATE", pair, nil)
	assert.NoError(t, err)
}

func testAtomicPutWithSlashSuffixKey(t *testing.T, kv store.Store) {
	k1 := "testAtomicPutWithSlashSuffixKey/key/"
	err := kv.AtomicPut(context.TODO(), k1, "", nil, nil)
	assert.Nil(t, err)
}

func testAtomicDelete(t *testing.T, kv store.Store) {
	key := "testAtomicDelete"
	value := "world"

	// Put the key
	err := kv.Put(context.TODO(), key, value, nil)
	assert.NoError(t, err)

	// Get should return the value and an incremented index
	pair, err := kv.Get(context.TODO(), key)
	assert.NoError(t, err)
	if assert.NotNil(t, pair) {
		assert.NotNil(t, pair.Value)
	}
	assert.Equal(t, pair.Value, value)
	assert.NotEqual(t, pair.Index, 0)

	tempIndex := pair.Index

	// AtomicDelete should fail
	pair.Index = 6744
	err = kv.AtomicDelete(context.TODO(), key, pair)
	assert.Error(t, err)

	// AtomicDelete should fail
	err = kv.AtomicDelete(context.TODO(), key, nil)
	assert.Error(t, err)

	// AtomicDelete should succeed
	pair.Index = tempIndex
	err = kv.AtomicDelete(context.TODO(), key, pair)
	assert.NoError(t, err)

	// Delete a non-existent key; should fail
	err = kv.AtomicDelete(context.TODO(), key, pair)
	assert.Error(t, store.ErrKeyNotFound)
}

func testLockUnlock(t *testing.T, kv store.Store) {
	key := "testLockUnlock"
	value := "bar"

	// We should be able to create a new lock on key
	lock := kv.NewLock(key, &store.LockOptions{Value: value, TTL: 2 * time.Second})
	assert.NotNil(t, lock)

	// Lock should successfully succeed or block
	lock.Lock(context.TODO())

	// Get should work
	pair, err := kv.Get(context.TODO(), key)
	assert.NoError(t, err)
	if assert.NotNil(t, pair) {
		assert.NotNil(t, pair.Value)
	}
	assert.Equal(t, pair.Value, value)
	assert.NotEqual(t, pair.Index, 0)

	// Unlock should succeed
	lock.Unlock(context.TODO())

	// Lock should succeed again
	lock.Lock(context.TODO())

	// Get should work
	pair, err = kv.Get(context.TODO(), key)
	assert.NoError(t, err)
	if assert.NotNil(t, pair) {
		assert.NotNil(t, pair.Value)
	}
	assert.Equal(t, pair.Value, value)
	assert.NotEqual(t, pair.Index, 0)

	lock.Unlock(context.TODO())
	assert.NoError(t, err)
}

func testLockUnlockV3(t *testing.T, kv store.Store) {
	key := "testLockUnlockV3"

	// We should be able to create a new lock on key
	lock := kv.NewLock(key, nil)
	assert.NotNil(t, lock)

	// Lock should successfully succeed
	err := lock.Lock(context.TODO())
	assert.NoError(t, err)

	// Unlock should succeed
	err = lock.Unlock(context.TODO())
	assert.NoError(t, err)

	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	// Lock should succeed again
	err = lock.Lock(ctx)
	assert.NoError(t, err)

	// Lock should succeed after 3s timeout
	<-time.After(time.Second * 3)
	err = lock.Lock(context.TODO())
	assert.NoError(t, err)

	unlockChan := make(chan struct{})
	notifyChan := make(chan struct{})
	go func() {
		unlockChan <- struct{}{}
	}()

	go func() {
		// Lock should block
		lockNew := kv.NewLock(key, nil)
		assert.NotNil(t, lockNew)
		lockNew.Lock(context.TODO())
		if _, ok := <-unlockChan; !ok {
			assert.FailNow(t, "should not get lock", "")
		}
		close(notifyChan)
	}()

	time.Sleep(time.Second)
	err = lock.Unlock(context.TODO())
	assert.NoError(t, err)
	time.Sleep(time.Second)
	<-notifyChan
	close(unlockChan)
}

func testLockTTL(t *testing.T, kv store.Store, otherConn store.Store) {
	key := "testLockTTL"
	value := "bar"

	renewCh := make(chan struct{})

	// We should be able to create a new lock on key
	lock := otherConn.NewLock(key, &store.LockOptions{
		Value:     value,
		TTL:       2 * time.Second,
		RenewLock: renewCh,
	})

	// Lock should successfully succeed
	lock.Lock(context.TODO())

	// Get should work
	pair, err := otherConn.Get(context.TODO(), key)
	assert.NoError(t, err)
	if assert.NotNil(t, pair) {
		assert.NotNil(t, pair.Value)
	}
	assert.Equal(t, pair.Value, value)
	assert.NotEqual(t, pair.Index, 0)

	time.Sleep(3 * time.Second)

	done := make(chan struct{})
	value = "foobar"

	// Create a new lock with another connection
	lockNew := otherConn.NewLock(
		key,
		&store.LockOptions{
			Value: value,
			TTL:   3 * time.Second,
		},
	)
	assert.NotNil(t, lockNew)

	// Lock should block, the session on the lock
	// is still active and renewed periodically
	go func(<-chan struct{}) {
		lockNew.Lock(context.TODO())
		pair, err = kv.Get(context.TODO(), key)
		assert.NoError(t, err)
		if assert.NotNil(t, pair) {
			assert.NotNil(t, pair.Value)
		}
		assert.Equal(t, pair.Value, value)
		assert.NotEqual(t, pair.Index, 0)
		lockNew.Unlock(context.TODO())
		done <- struct{}{}
	}(done)

	select {
	case <-done:
		t.Fatal("Lock succeeded on a key that is supposed to be locked by another client")
	case <-time.After(4 * time.Second):
		// Stop requesting the lock as we are blocked as expected
		lock.Unlock(context.TODO())
		break
	}

	locked := make(chan struct{})
	valueNew := "bar"

	// Lock should now succeed for the other client
	go func(<-chan struct{}) {
		lock.Lock(context.TODO())
		locked <- struct{}{}
	}(locked)

	select {
	case <-locked:
		break
	case <-time.After(4 * time.Second):
		t.Fatal("Unable to take the lock, timed out")
	}

	// Get should work with the new value
	pair, err = kv.Get(context.TODO(), key)
	assert.NoError(t, err)
	if assert.NotNil(t, pair) {
		assert.NotNil(t, pair.Value)
	}
	assert.Equal(t, pair.Value, valueNew)
	assert.NotEqual(t, pair.Index, 0)

	lock.Unlock(context.TODO())
}

func testLockTTLV3(t *testing.T, kv store.Store, otherConn store.Store) {
	key := "testLockTTL"

	// We should be able to create a new lock on key
	lock1 := otherConn.NewLock(key, &store.LockOptions{TTL: 2 * time.Second})
	assert.NotNil(t, lock1)

	// Lock should successfully succeed
	lock1.Lock(context.TODO())

	// Create a new lock with another connection
	lock2 := otherConn.NewLock(key, &store.LockOptions{TTL: 3 * time.Second})
	assert.NotNil(t, lock2)

	done := make(chan struct{})

	// lock2 should block, the session on the lock
	// is still active and renewed periodically
	go func(<-chan struct{}) {
		lock2.Lock(context.TODO())
		done <- struct{}{}
	}(done)

	select {
	case <-done:
		t.Fatal("Lock succeeded on a key that is supposed to be locked by another client")
	case <-time.After(4 * time.Second):
		// Stop requesting the lock as we are blocked as expected
		// Close the connection
		otherConn.Close()
		break
	}

	locked := make(chan struct{})

	// Lock should now succeed for the other client
	go func(<-chan struct{}) {
		lock1.Lock(context.TODO())
		locked <- struct{}{}
	}(locked)

	select {
	case <-locked:
		break
	case <-time.After(4 * time.Second):
		t.Fatal("Unable to take the lock, timed out")
	}

	lock1.Unlock(context.TODO())
}

func testPutTTL(t *testing.T, kv store.Store, otherConn store.Store) {
	firstKey := "testPutTTL"
	firstValue := "foo"

	secondKey := "second"
	secondValue := "bar"

	// Put the first key with the Ephemeral flag
	err := otherConn.Put(context.TODO(), firstKey, firstValue, &store.WriteOptions{TTL: 2 * time.Second})
	assert.NoError(t, err)

	// Put a second key with the Ephemeral flag
	err = otherConn.Put(context.TODO(), secondKey, secondValue, &store.WriteOptions{TTL: 2 * time.Second})
	assert.NoError(t, err)

	// Get on firstKey should work
	pair, err := kv.Get(context.TODO(), firstKey)
	assert.NoError(t, err)
	assert.NotNil(t, pair)

	// Get on secondKey should work
	pair, err = kv.Get(context.TODO(), secondKey)
	assert.NoError(t, err)
	assert.NotNil(t, pair)

	// Close the connection
	otherConn.Close()

	// Let the session expire
	time.Sleep(3 * time.Second)

	// Get on firstKey shouldn't work
	pair, err = kv.Get(context.TODO(), firstKey)
	assert.Error(t, err)
	assert.Nil(t, pair)

	// Get on secondKey shouldn't work
	pair, err = kv.Get(context.TODO(), secondKey)
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
	err := kv.Put(context.TODO(), firstKey, firstValue, nil)
	assert.NoError(t, err)

	// Put the second key
	err = kv.Put(context.TODO(), secondKey, secondValue, nil)
	assert.NoError(t, err)

	// List should work and return the two correct values
	for _, parent := range []string{prefix, prefix + "/"} {
		pairs, err := kv.List(context.TODO(), parent)
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
	pairs, err := kv.List(context.TODO(), "idontexist")
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
	err := kv.Put(context.TODO(), firstKey, firstValue, nil)
	assert.NoError(t, err)

	// Put the second key
	err = kv.Put(context.TODO(), secondKey, secondValue, nil)
	assert.NoError(t, err)

	// Get should work on the first Key
	pair, err := kv.Get(context.TODO(), firstKey)
	assert.NoError(t, err)
	if assert.NotNil(t, pair) {
		assert.NotNil(t, pair.Value)
	}
	assert.Equal(t, pair.Value, firstValue)
	assert.NotEqual(t, pair.Index, 0)

	// Get should work on the second Key
	pair, err = kv.Get(context.TODO(), secondKey)
	assert.NoError(t, err)
	if assert.NotNil(t, pair) {
		assert.NotNil(t, pair.Value)
	}
	assert.Equal(t, pair.Value, secondValue)
	assert.NotEqual(t, pair.Index, 0)

	// Delete Values under directory `nodes`
	err = kv.DeleteTree(context.TODO(), prefix)
	assert.NoError(t, err)

	// Get should fail on both keys
	pair, err = kv.Get(context.TODO(), firstKey)
	assert.Error(t, err)
	assert.Nil(t, pair)

	pair, err = kv.Get(context.TODO(), secondKey)
	assert.Error(t, err)
	assert.Nil(t, pair)
}

// RunCleanup cleans up keys introduced by the tests
func RunCleanup(t *testing.T, kv store.Store) {
	for _, key := range []string{
		"testAtomicPutWithSlashSuffixKey",
		"testPutGetDeleteExistsUpdateCreate",
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
		err := kv.DeleteTree(context.TODO(), key)
		assert.True(t, err == nil || err == store.ErrKeyNotFound, fmt.Sprintf("failed to delete tree key %s: %v", key, err))
		err = kv.Delete(context.TODO(), key)
		assert.True(t, err == nil || err == store.ErrKeyNotFound, fmt.Sprintf("failed to delete key %s: %v", key, err))
	}
}
