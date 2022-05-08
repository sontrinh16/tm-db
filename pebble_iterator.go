//go:build pebbledb

package db

import (
	"bytes"

	"github.com/cockroachdb/pebble"
)

type pebbleDBIterator struct {
	iterator  *pebble.Iterator
	isReverse bool
	isInvalid bool
}

var _ Iterator = (*pebbleDBIterator)(nil)

func newPebbleDBIterator(db *pebble.DB, start, end []byte, isReverse bool) *pebbleDBIterator {
	it := db.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	return &pebbleDBIterator{
		iterator:  it,
		isReverse: isReverse,
		isInvalid: false,
	}
}

// Domain implements Iterator.
func (itr *pebbleDBIterator) Domain() ([]byte, []byte) {
	return itr.iterator.RangeBounds()
}

// Valid implements Iterator.
func (itr *pebbleDBIterator) Valid() bool {
	// Once invalid, forever invalid.
	if itr.isInvalid {
		return false
	}

	// If source has error, invalid.
	if err := itr.iterator.Error(); err != nil {
		itr.isInvalid = true

		return false
	}

	// If source is invalid, invalid.
	if !itr.iterator.Valid() {
		itr.isInvalid = true

		return false
	}

	start, end := itr.iterator.RangeBounds()
	// If key is end or past it, invalid.
	key := itr.iterator.Key()
	if itr.isReverse {
		if start != nil && bytes.Compare(key, start) < 0 {
			itr.isInvalid = true

			return false
		}
	} else {
		if end != nil && bytes.Compare(end, key) <= 0 {
			itr.isInvalid = true

			return false
		}
	}

	// It's valid.
	return true
}

// Key implements Iterator.
func (itr *pebbleDBIterator) Key() []byte {
	itr.assertIsValid()
	return itr.iterator.Key()
}

// Value implements Iterator.
func (itr *pebbleDBIterator) Value() []byte {
	itr.assertIsValid()
	return itr.iterator.Value()
}

// Next implements Iterator.
func (itr pebbleDBIterator) Next() {
	itr.assertIsValid()
	if itr.isReverse {
		itr.iterator.Prev()
	} else {
		itr.iterator.Next()
	}
}

// Error implements Iterator.
func (itr *pebbleDBIterator) Error() error {
	return itr.iterator.Error()
}

// Close implements Iterator.
func (itr *pebbleDBIterator) Close() error {
	err := itr.iterator.Close()
	if err != nil {
		return err
	}
	return nil
}

func (itr *pebbleDBIterator) assertIsValid() {
	if !itr.Valid() {
		panic("iterator is invalid")
	}
}
