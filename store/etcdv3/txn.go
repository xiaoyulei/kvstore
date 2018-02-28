package etcdv3

import (
	"context"

	"github.com/YuleiXiao/kvstore/store"
	etcd "github.com/coreos/etcd/clientv3"
)

type txn struct {
	ctx    context.Context
	client *etcd.Client
	txn    etcd.Txn

	cmp      []etcd.Cmp
	success  []etcd.Op
	Fail     []etcd.Op
	isFailOp bool
}

func (t *txn) Begin() {
	t.txn = t.client.Txn(t.ctx)
	t.cmp = nil
	t.success = nil
	t.Fail = nil
	t.isFailOp = false
}

func (t *txn) Commit() (*store.TxnResponse, error) {
	resp, err := t.txn.If(t.cmp...).Then(t.success...).Else(t.Fail...).Commit()
	if err != nil {
		return nil, err
	}

	txnResp := &store.TxnResponse{}
	txnResp.CompareSuccess = resp.Succeeded
	txnResp.Revision = uint64(resp.Header.Revision)
	for _, r := range resp.Responses {
		opResp := &store.OpResponse{}

		if putResp := r.GetResponsePut(); putResp != nil {
			//TODO: Is there anything need handle here
		} else if rangeDeleteResp := r.GetResponseDeleteRange(); rangeDeleteResp != nil {
			//TODO: Is there anything need handle here
		} else if rangeResp := r.GetResponseRange(); rangeResp != nil {
			for _, kv := range rangeResp.Kvs {
				opResp.Pairs = append(opResp.Pairs, &store.KVPair{
					Key:     string(kv.Key),
					Value:   string(kv.Value),
					Index:   uint64(kv.ModRevision),
					Version: uint64(kv.Version),
					Lease:   uint64(kv.Lease),
				})
			}
		}
		txnResp.Responses = append(txnResp.Responses, opResp)
	}

	return txnResp, nil
}

func (t *txn) IfValue(key, operator string, value string) {
	t.cmp = append(t.cmp, etcd.Compare(etcd.Value(key), operator, value))
}

func (t *txn) IfVersion(key, operator string, version uint64) {
	t.cmp = append(t.cmp, etcd.Compare(etcd.Version(key), operator, int64(version)))
}

func (t *txn) IfCreateRevision(key, operator string, revision uint64) {
	t.cmp = append(t.cmp, etcd.Compare(etcd.CreateRevision(key), operator, int64(revision)))
}

func (t *txn) IfModifyRevision(key, operator string, revision uint64) {
	t.cmp = append(t.cmp, etcd.Compare(etcd.ModRevision(key), operator, int64(revision)))
}

func (t *txn) Put(key, value string, options *store.WriteOptions) {
	var op etcd.Op
	if options != nil {
		leaseResp, err := t.client.Grant(t.ctx, int64(options.TTL.Seconds()))
		if err != nil {
			return
		}

		op = etcd.OpPut(key, value, etcd.WithLease(leaseResp.ID))
	} else {
		op = etcd.OpPut(key, value)
	}

	if t.isFailOp {
		t.Fail = append(t.Fail, op)
		return
	}
	t.success = append(t.success, op)
}

func (t *txn) Get(key string) {
	if t.isFailOp {
		t.Fail = append(t.Fail, etcd.OpGet(key))
		return
	}
	t.success = append(t.success, etcd.OpGet(key))
}

func (t *txn) List(dir string) {
	if t.isFailOp {
		t.Fail = append(t.Fail, etcd.OpGet(dir, etcd.WithPrefix()))
		return
	}
	t.success = append(t.success, etcd.OpGet(dir, etcd.WithPrefix()))
}

func (t *txn) Delete(key string) {
	if t.isFailOp {
		t.Fail = append(t.Fail, etcd.OpDelete(key))
		return
	}
	t.success = append(t.success, etcd.OpDelete(key))
}

func (t *txn) DeleteTree(key string) {
	if t.isFailOp {
		t.Fail = append(t.Fail, etcd.OpDelete(key, etcd.WithPrefix()))
		return
	}
	t.success = append(t.success, etcd.OpDelete(key, etcd.WithPrefix()))
}

func (t *txn) Else() {
	t.isFailOp = true
}
