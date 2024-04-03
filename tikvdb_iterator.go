package db

import (
	"bytes"
	"context"
	"fmt"

	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
)

type tikvDBIterator struct {
	source    tikv.Iterator
	txn       *transaction.KVTxn
	prefix    []byte
	start     []byte
	end       []byte
	isReverse bool
	isValid   bool
	err       error
}

var _ Iterator = (*tikvDBIterator)(nil)

// newTikvDBIterator All key-value records are logically arranged in sorted order.
// The iterators allow applications to do range scans on TiKV.
// The iterator yields records in the range [start, end).
func newTikvDBIterator(txn *transaction.KVTxn, prefix []byte, start, end []byte, isReverse bool) (*tikvDBIterator, error) {
	var source tikv.Iterator
	var err error
	//var staKey = byte('0')
	var endKey = []byte("~")
	var iterator = &tikvDBIterator{
		txn:       txn,
		prefix:    prefix,
		start:     start,
		end:       end,
		isReverse: isReverse,
		isValid:   true,
	}

	// 反向迭代器中参数是开始的 key, 结束的 key 需要在 NEXT() 中进行过判断
	var startKey []byte
	if isReverse {
		if end == nil {
			startKey = iterator.getTikvKey(endKey)
		} else {
			startKey = iterator.getTikvKey(end)
		}

		source, err = txn.IterReverse(startKey)
		if err != nil {
			return nil, err
		}

		if !source.Valid() {
			source, err = txn.IterReverse(iterator.getTikvKey(endKey))
			if err != nil {
				return nil, err
			}
		}
	} else {
		if start == nil {
			startKey = iterator.getTikvKey(nil)
		} else {
			startKey = iterator.getTikvKey(start)
		}

		source, err = txn.Iter(startKey, iterator.getTikvKey(endKey))
		if err != nil {
			return nil, err
		}
	}

	iterator.source = source
	return iterator, nil
}

func (itr *tikvDBIterator) Domain() (start []byte, end []byte) {
	return itr.start, itr.end
}

func (itr *tikvDBIterator) Valid() bool {
	var start = itr.start
	var end = itr.end
	var key = itr.source.Key()

	// determine if the limit of `start` or `end` has been reached
	if itr.isReverse {
		if start != nil && bytes.Compare(key, itr.getTikvKey(start)) < 0 {
			itr.isValid = false
			return false
		}
	} else {
		if end != nil && bytes.Compare(itr.getTikvKey(end), key) <= 0 {
			itr.isValid = false
			return false
		}
	}

	// determine if there are already other errors or if the prefix is out of scope
	if !itr.isValid || itr.err != nil || !itr.source.Valid() {
		return false
	}

	if len(key) < len(itr.prefix) || !bytes.Equal(key[:len(itr.prefix)], itr.prefix) {
		itr.isValid = false
		itr.err = fmt.Errorf("received invalid key from backend: %x (expected prefix %x)", key, itr.prefix)
		return false
	}

	return true
}

func (itr *tikvDBIterator) Next() {
	itr.assertIsValid()
	err := itr.source.Next()
	if err != nil {
		itr.err = err
	}

	if !itr.source.Valid() || !bytes.HasPrefix(itr.source.Key(), itr.prefix) {
		itr.isValid = false
	} else if bytes.Equal(itr.source.Key(), itr.prefix) {
		// Empty keys are not allowed, so if a key exists in the database that exactly matches the
		// prefix we need to skip it.
		itr.Next()
	}
}

func (itr *tikvDBIterator) Key() (key []byte) {
	// Key returns a copy of the current key.
	itr.assertIsValid()
	fullKey := itr.source.Key()
	return fullKey[len(itr.prefix):]
}

func (itr *tikvDBIterator) Value() (value []byte) {
	// Value returns a copy of the current value.
	itr.assertIsValid()
	return checkEmptyValue(itr.source.Value())
}

func (itr *tikvDBIterator) Error() error {
	return itr.err
}

func (itr *tikvDBIterator) Close() error {
	itr.source.Close()
	return itr.txn.Commit(context.Background())
}

func (itr *tikvDBIterator) assertIsValid() {
	if !itr.Valid() {
		panic("iterator is invalid")
	}
}

func (itr *tikvDBIterator) getTikvKey(key []byte) []byte {
	return append(cp(itr.prefix), key...)
}
