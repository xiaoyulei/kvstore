package etcdv3

import (
	"fmt"
	"testing"
	"time"

	"github.com/YuleiXiao/kvstore/store"

	"golang.org/x/net/context"
)

func TestEtcdTxn(t *testing.T) {
	kv := makeEtcdClient(t)
	defer kv.Close()

	testNewTxn(t, kv)
	testTxn(t, kv)
}

func testNewTxn(t *testing.T, kv store.Store) {
	_, err := kv.NewTxn(context.Background())
	if err != nil {
		t.Errorf("Txn should be supported in etcdv3. %v", err)
	}
}

func testTxn(t *testing.T, kv store.Store) {
	txn, err := kv.NewTxn(context.Background())
	if err != nil {
		t.Errorf("Txn should be supported in etcdv3. %v", err)
	}

	key := "/txn/key"
	value := "txn_value"

	key2 := "/txn/key2"
	value2 := "txn_value2"

	// simple transaction
	txn.Begin()
	txn.Put(key, value, nil)
	txn.Get(key)
	resp, err := txn.Commit()
	if err != nil {
		t.Errorf("txn execute fail. %v", err)
		return
	}
	if len(resp.Responses) != 2 {
		t.Errorf("txn response count not same with request. %v", resp)
	}
	if resp.Responses[1].Pairs[0].Value != value {
		t.Errorf("txn get value not correct. %v", resp)
	}

	// simple transaction
	txn.Begin()
	txn.Put(key, value, &store.WriteOptions{TTL: time.Second})
	time.Sleep(5 * time.Second)
	txn.Get(key)
	resp, err = txn.Commit()
	if err == nil {
		t.Errorf("txn execute with expire lease should fail. %v", resp)
	}

	// transaction with compare value
	txn.Begin()
	txn.IfValue(key, "=", value)
	txn.Put(key2, value2, nil)
	txn.Delete(key)
	txn.Get(key)
	txn.Get(key2)
	resp, err = txn.Commit()
	if err != nil {
		t.Errorf("txn execute fail. %v", err)
		return
	}
	if len(resp.Responses) != 4 {
		t.Errorf("txn response count not same with request. %v", resp)
	}
	if resp.Responses[2].Pairs != nil {
		t.Errorf("txn get non-exist key. %v", resp)
	}
	if resp.Responses[3].Pairs[0].Value != value2 {
		t.Errorf("txn get value not correct. %v", resp)
	}

	// transaction with modify revision and compare fail
	txn.Begin()
	txn.IfModifyRevision(key2, "=", 0)
	txn.Else()
	txn.Put(key2, value2, nil)
	txn.Delete(key)
	txn.Get(key)
	txn.Get(key2)
	resp, err = txn.Commit()
	if err != nil {
		t.Errorf("txn execute fail. %v", err)
		return
	}
	if len(resp.Responses) != 4 {
		t.Errorf("txn response count not same with request. %v", resp)
	}
	if resp.Responses[2].Pairs != nil {
		t.Errorf("txn get non-exist key. %v", resp)
	}
	if resp.Responses[3].Pairs[0].Value != value2 {
		t.Errorf("txn get value not correct. %v", resp)
	}

	// transaction with modify revision and compare fail
	txn.Begin()
	txn.IfCreateRevision(key2, "!=", 0)
	txn.Delete(key)
	txn.Delete(key2)
	txn.Get(key)
	txn.Get(key2)
	resp, err = txn.Commit()
	if err != nil {
		t.Errorf("txn execute fail. %v", err)
		return
	}
	if len(resp.Responses) != 4 {
		t.Errorf("txn response count not same with request. %v", resp)
	}
	if resp.Responses[2].Pairs != nil {
		t.Errorf("txn get non-exist key. %v", resp)
	}
	if resp.Responses[3].Pairs != nil {
		t.Errorf("txn get non-exist key. %v", resp)
	}

	// transaction with List and lease
	txn.Begin()
	for i := 0; i < 10; i++ {
		key3 := fmt.Sprintf("/txn/key%d", i)
		value3 := fmt.Sprintf("txn_value%d", i)

		if i < 3 {
			txn.Put(key3, value3, &store.WriteOptions{TTL: time.Second})
		} else {
			txn.Put(key3, value3, nil)
		}
	}
	txn.List("/txn")
	resp, err = txn.Commit()
	if err != nil {
		t.Errorf("txn execute fail. %v", err)
		return
	}
	if len(resp.Responses) != 11 {
		t.Errorf("txn response count not same with request. %v", resp)
	}
	if len(resp.Responses[10].Pairs) != 10 {
		t.Errorf("txn list result not correct. %v", resp)
	}

	// transaction with DeleteTree
	txn.Begin()
	txn.DeleteTree("/txn")
	txn.List("/txn")
	resp, err = txn.Commit()
	if err != nil {
		t.Errorf("txn execute fail. %v", err)
		return
	}
	if len(resp.Responses) != 2 {
		t.Errorf("txn response count not same with request. %v", resp)
	}
	if resp.Responses[1].Pairs != nil {
		t.Errorf("txn list result not correct. %v", resp)
	}
}
