package db

import (
	"context"
	"fmt"
	"sync"

	"github.com/spf13/viper"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/txnkv"
)

// value is a special identifier for null
var emptyStringTag = []byte("NULL")

func init() {
	dbCreator := func(name string, dir string) (DB, error) {
		return NewTikvDB(name, dir)
	}
	registerDBCreator(TikvDBBackend, dbCreator, false)
}

type TikvDB struct {
	txn  *txnkv.Client
	lock sync.RWMutex
	name string
	dir  string
}

var _ DB = (*TikvDB)(nil)

func NewTikvDB(name string, dir string) (*TikvDB, error) {
	addrs := viper.GetStringSlice(FlagTikvDBAddrs)
	//params := parseOptParams(viper.GetString(FlagTikvDBOpts))

	return NewTikvDBWithOpts(name, dir, addrs, nil)
}

func NewTikvDBWithOpts(name string, dir string, pdAddrs []string, _ ...txnkv.ClientOpt) (*TikvDB, error) {
	// Initializing the tikv client
	txnClient, err := txnkv.NewClient(pdAddrs)
	if err != nil {
		return nil, err
	}

	database := &TikvDB{
		txn:  txnClient,
		name: name,
		dir:  dir,
	}

	// Performs prefix data check. If the prefix exists, an error is returned.
	//txn, err := database.txn.Begin()
	//if err != nil {
	//	return nil, err
	//}
	//defer txn.Commit(context.Background())
	//_, err = txn.Get(context.Background(), database.getTikvStateKey())
	//if err == nil {
	//	return nil, fmt.Errorf("database '%s/%s' is already in use", dir, name)
	//}
	//if err != nil {
	//	if !tikverr.IsErrNotFound(err) {
	//		return nil, fmt.Errorf("database '%s/%s' is already in use", dir, name)
	//	}
	//}
	//err = txn.Set(database.getTikvStateKey(), []byte("1"))
	//if err != nil {
	//	return nil, err
	//}

	return database, nil
}

func (t *TikvDB) Get(key []byte) ([]byte, error) {
	txn, err := t.txn.Begin()
	if err != nil {
		return nil, err
	}
	defer func() {
		err = txn.Commit(context.Background())
	}()

	val, err := txn.Get(context.Background(), t.getTikvKey(key))
	if err != nil {
		if tikverr.IsErrNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return checkEmptyValue(val), err
}

func (t *TikvDB) Has(key []byte) (bool, error) {
	txn, err := t.txn.Begin()
	if err != nil {
		return false, err
	}
	defer func() {
		err = txn.Commit(context.Background())
	}()

	_, err = txn.Get(context.Background(), t.getTikvKey(key))
	if err == nil {
		return true, nil
	}
	if tikverr.IsErrNotFound(err) {
		return false, nil
	}
	return false, err
}

func (t *TikvDB) Set(key []byte, value []byte) error {
	return t.setKV(key, value)
}

func (t *TikvDB) SetSync(key []byte, value []byte) error {
	return t.setKV(key, value)
}

func (t *TikvDB) setKV(key []byte, value []byte) error {
	txn, err := t.txn.Begin()
	if err != nil {
		return err
	}
	defer func() {
		err = txn.Commit(context.Background())
	}()

	err = txn.Set(t.getTikvKey(key), setNotEmptyValue(value))
	if err != nil {
		return err
	}
	return err
}

func (t *TikvDB) Delete(key []byte) error {
	return t.deleteKey(key)
}

func (t *TikvDB) DeleteSync(key []byte) error {
	return t.deleteKey(key)
}

func (t *TikvDB) deleteKey(key []byte) error {
	txn, err := t.txn.Begin()
	if err != nil {
		return err
	}
	defer func() {
		err = txn.Commit(context.Background())
	}()

	err = txn.Delete(t.getTikvKey(key))
	if err != nil {
		return err
	}
	return err
}

func (t *TikvDB) Close() (err error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	txn, err := t.txn.Begin()
	if err != nil {
		return err
	}
	defer func() {
		err = txn.Commit(context.Background())
	}()
	err = txn.Delete(t.getTikvStateKey())
	if err != nil {
		return err
	}
	return t.txn.Close()
}

func (t *TikvDB) Print() error {
	fmt.Printf("prefix: %v\n", t.tikvStoreKeyPrefix())
	start := t.getTikvKey(nil)
	end := t.getTikvKey([]byte("~"))

	itr, err := t.Iterator(start, end)
	if err != nil {
		return err
	}

	for ; itr.Valid(); itr.Next() {
		key := itr.Key()
		value := itr.Value()
		fmt.Printf("[%X]:\t[%X]\n", key, value)
	}
	return itr.Close()
}

func (t *TikvDB) Stats() map[string]string {
	stats := make(map[string]string)
	stats["database.type"] = "tikvDB"
	stats["database.prefix"] = t.tikvStoreKeyPrefix()
	return stats
}

func (t *TikvDB) NewBatch() Batch {
	return newTikvDBBatch(t, t.tikvStoreKeyPrefix())
}

func (t *TikvDB) Iterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}

	txn, err := t.txn.Begin()
	if err != nil {
		return nil, err
	}

	return newTikvDBIterator(txn, []byte(t.tikvStoreKeyPrefix()), start, end, false)
}

func (t *TikvDB) ReverseIterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}

	txn, err := t.txn.Begin()
	if err != nil {
		return nil, err
	}

	return newTikvDBIterator(txn, []byte(t.tikvStoreKeyPrefix()), start, end, true)
}

func (t *TikvDB) tikvStoreKeyPrefix() string {
	return fmt.Sprintf("%s/%s/", t.dir, t.name)
}

func (t *TikvDB) getTikvKey(key []byte) []byte {
	return append([]byte(t.tikvStoreKeyPrefix()), key...)
}

func (t *TikvDB) getTikvStateKey() []byte {
	return []byte(fmt.Sprintf("tikv.state/%s/%s/tikv.state", t.dir, t.name))
}
