package db

import "context"

// keyvalue is a key-value tuple tagged with a deletion field to allow creating
// memory-database write batches.
type keyvalue struct {
	key    []byte
	value  []byte
	delete bool
}

// tikvDBBatch is a write-only memory batch that commits changes to its host
// database when Write is called. A batch cannot be used concurrently.
type tikvDBBatch struct {
	db     *TikvDB
	writes []keyvalue
	prefix string
}

var _ Batch = (*tikvDBBatch)(nil)

// newTikvDBBatch creates a new batch object.
func newTikvDBBatch(db *TikvDB, prefix string) *tikvDBBatch {
	return &tikvDBBatch{
		db:     db,
		prefix: prefix,
	}
}

// Set inserts the given value into the batch for later committing.
func (b *tikvDBBatch) Set(key, value []byte) error {
	b.writes = append(b.writes, keyvalue{b.db.getTikvKey(key), value, false})
	return nil
}

// Delete inserts the key removal into the batch for later committing.
func (b *tikvDBBatch) Delete(key []byte) error {
	b.writes = append(b.writes, keyvalue{b.db.getTikvKey(key), nil, true})
	return nil
}

func (b *tikvDBBatch) Write() error {
	return b.write(false)
}

func (b *tikvDBBatch) WriteSync() error {
	return b.write(true)
}

// write writes the batch to TiKV.
func (b *tikvDBBatch) write(_ bool) error {
	txn, err := b.db.txn.Begin()
	if err != nil {
		return err
	}

	for _, keyValue := range b.writes {
		if keyValue.delete {
			if err := txn.Delete(keyValue.key); err != nil {
				return err
			}
			continue
		}
		if err := txn.Set(keyValue.key, keyValue.value); err != nil {
			return err
		}
	}
	return txn.Commit(context.Background())
}

// Close resets the batch for reuse.
func (b *tikvDBBatch) Close() error {
	b.writes = b.writes[:0]
	return nil
}
