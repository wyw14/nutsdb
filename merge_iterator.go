// Copyright 2026 The nutsdb Author. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nutsdb

import (
	"bytes"
	"sort"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/tidwall/btree"
)

// MergeIteratorOptions configures a MergeIterator.
//
// The traversed key range is the intersection of [Start, End] (bounds) and the
// optional Prefix filter. A nil Start disables the lower bound and a nil End
// disables the upper bound. When a bound is set, the corresponding inclusive
// flag selects a closed (true) or open (false) boundary. Bounds and iteration
// direction are evaluated with the comparator configured on the database
// (Options.LessFunc), so they stay correct under custom non-lexicographic
// comparators.
//
// Prefix is always applied as a pure byte filter (bytes.HasPrefix). It never
// narrows the index cursor and never terminates a scan early, which matters
// under a custom comparator where keys sharing a byte prefix are not
// necessarily contiguous in comparator order.
type MergeIteratorOptions struct {
	// Reverse selects descending (true) or ascending (false) traversal.
	Reverse bool

	// Prefix optionally keeps only keys with this byte prefix.
	Prefix []byte

	// Start is the lower bound of the range; nil means no lower bound.
	Start []byte

	// End is the upper bound of the range; nil means no upper bound.
	End []byte

	// StartInclusive keeps Start itself when true, excludes it when false.
	// Ignored when Start is nil.
	StartInclusive bool

	// EndInclusive keeps End itself when true, excludes it when false.
	// Ignored when End is nil.
	EndInclusive bool
}

// pendingMergeItem is one entry of the transaction-local pending layer after
// it has been snapshotted and sorted. deleted/expired entries act as
// tombstones that mask any older committed value for the same key.
type pendingMergeItem struct {
	key     []byte
	entry   *core.Entry
	deleted bool
	expired bool
}

// MergeIterator traverses the merged view of the committed BTree index and the
// transaction-local pending write layer as it existed when the iterator was
// created.
//
// Merge semantics for a key:
//   - If the key has a pending write, the pending state wins. A live pending
//     value overrides the committed value; a pending delete or an expired
//     pending value masks the committed value entirely, so an old value can
//     never re-appear after its newer version was filtered out.
//   - Otherwise the committed index value is returned when it is still valid
//     (not expired).
//
// Expired and deleted entries never occupy a returned position.
//
// Snapshot and invalidation: the iterator records the owning transaction's
// write version at creation. Any subsequent key write or bucket write in the
// same transaction bumps the version and every later iterator operation fails
// with ErrMergeIteratorStale instead of mixing two pending versions. After
// Commit or Rollback the transaction is closed and every operation fails with
// ErrTxClosed without dereferencing the cleaned-up database.
//
// Resource and value lifecycle:
//   - Key returns the underlying key slice owned by the index/pending entry.
//     It stays valid until Release is called or the transaction ends; callers
//     must not retain it beyond that point and must not mutate it.
//   - Value is read lazily. Pending values come straight from the in-memory
//     entry. Committed values follow the configured index mode: in RAM mode
//     the in-memory record value is returned, while Hint mode reads from disk
//     (or the entry cache) on every call, so each Value call may return a
//     fresh slice.
//   - Release closes the underlying index cursor. It is idempotent and safe to
//     call after the transaction has ended; the cursor is released exactly
//     once and is never leaked.
type MergeIterator struct {
	tx      *Tx
	bucket  core.BucketName
	options MergeIteratorOptions
	less    func(a, b []byte) bool

	// snapshotVersion is the transaction write version captured at creation.
	snapshotVersion uint64
	released        bool

	// pending is the sorted auxiliary layer built from the P pending keys of
	// this bucket in O(P log P). It is a creation-time snapshot and is never
	// mutated afterwards.
	pending []pendingMergeItem

	// indexCursor walks the committed BTree. It is absent (hasIndex == false)
	// for buckets created inside this transaction that have no committed tree
	// yet, so a brand-new bucket can still be traversed through its pending
	// layer.
	indexCursor btree.IterG[*core.Item[core.Record]]
	hasIndex    bool

	// Cursor state over the two sorted sources.
	pPos  int
	pOK   bool
	iOK   bool
	iItem *core.Item[core.Record]

	// Current merged position.
	valid      bool
	curKey     []byte
	curPending *core.Entry
	curRecord  *core.Record

	// indexVisitedKeys records every key the committed index cursor lands on.
	// It exists to verify in tests that the index walk is restricted to the
	// requested key range instead of copying/scanning the whole tree.
	indexVisitedKeys [][]byte
}

// NewMergeIterator creates a MergeIterator over bucket using the given
// options. The iterator is positioned at the first item of the configured
// direction (equivalent to calling Rewind immediately).
//
// It returns ErrTxClosed when the transaction is already committed or rolled
// back, and ErrNotFoundBucket when the bucket does not exist or is deleted in
// the current transaction. Buckets created inside the transaction are
// supported. Release must be called when the iterator is no longer needed.
func NewMergeIterator(tx *Tx, bucket string, options MergeIteratorOptions) (*MergeIterator, error) {
	if tx == nil || tx.isClosed() || tx.db == nil {
		return nil, ErrTxClosed
	}

	status, b := tx.getBucketAndItsStatus(DataStructureBTree, bucket)
	if isBucketNotFoundStatus(status) {
		return nil, ErrNotFoundBucket
	}

	it := &MergeIterator{
		tx:              tx,
		bucket:          b.Name,
		options:         options,
		less:            tx.db.keyLessFunc(),
		snapshotVersion: tx.writeVersion,
		pPos:            -1,
	}

	// Build the O(P log P) sorted auxiliary layer from the creation-time
	// pending snapshot.
	it.pending = it.snapshotPending(b.Name)

	// Open the committed index cursor only when a committed tree exists.
	if idx, ok := tx.db.Index.BTree.exist(b.Id); ok {
		it.hasIndex = true
		it.indexCursor = idx.Iter()
	}

	if _, err := it.Rewind(); err != nil {
		it.releaseCursor()
		it.released = true
		return nil, err
	}

	return it, nil
}

// snapshotPending copies the pending BTree entries of bucket into a slice
// sorted with the database comparator and marks tombstones.
func (it *MergeIterator) snapshotPending(bucket core.BucketName) []pendingMergeItem {
	mp := it.tx.pendingWrites.entriesInBTree[bucket]
	if len(mp) == 0 {
		return nil
	}

	items := make([]pendingMergeItem, 0, len(mp))
	for _, entry := range mp {
		item := pendingMergeItem{key: entry.Key, entry: entry}
		switch {
		case entry.Meta.Flag == DataDeleteFlag:
			item.deleted = true
		case it.tx.db.ttlService.IsExpired(entry.Meta.TTL, entry.Meta.Timestamp):
			item.expired = true
		}
		items = append(items, item)
	}

	sort.Slice(items, func(i, j int) bool {
		return it.less(items[i].key, items[j].key)
	})
	return items
}

// Rewind repositions the iterator at the first item of its configured
// direction: the smallest in-range item when ascending, the largest when
// descending.
func (it *MergeIterator) Rewind() (bool, error) {
	if err := it.checkUsable(); err != nil {
		return false, err
	}
	if it.options.Reverse {
		return it.positionReverse(it.options.End)
	}
	return it.positionForward(it.options.Start)
}

// Seek moves the iterator to the first merged item that is at least target in
// ascending mode, or at most target in descending mode. The configured range
// is always respected, so the effective pivot is target intersected with
// [Start, End]. Prefix filtering still applies afterwards.
func (it *MergeIterator) Seek(target []byte) (bool, error) {
	if err := it.checkUsable(); err != nil {
		return false, err
	}

	if it.options.Reverse {
		// First item <= target, still not above End.
		upper := target
		if it.options.End != nil && it.cmp(target, it.options.End) > 0 {
			upper = it.options.End
		}
		return it.positionReverse(upper)
	}

	// First item >= target, still not below Start.
	lower := target
	if it.options.Start != nil && it.cmp(target, it.options.Start) < 0 {
		lower = it.options.Start
	}
	return it.positionForward(lower)
}

// Next advances to the following merged item in the configured direction.
// It returns false when no further item matches.
func (it *MergeIterator) Next() (bool, error) {
	if err := it.checkUsable(); err != nil {
		return false, err
	}
	if !it.valid {
		return false, nil
	}
	if it.options.Reverse {
		return it.advanceReverse()
	}
	return it.advanceForward()
}

// Valid reports whether the iterator currently points at a merged item.
func (it *MergeIterator) Valid() bool {
	return it.valid
}

// Key returns the key of the current item, or ErrKeyNotFound when the
// iterator is not positioned on an item. Stale/closed/released usage returns
// the corresponding explicit error.
func (it *MergeIterator) Key() ([]byte, error) {
	if err := it.checkUsable(); err != nil {
		return nil, err
	}
	if !it.valid {
		return nil, ErrKeyNotFound
	}
	return it.curKey, nil
}

// Value lazily reads the value of the current item. Pending values are served
// from memory while committed values are loaded through the database according
// to the configured entry index mode (RAM vs. Hint).
func (it *MergeIterator) Value() ([]byte, error) {
	if err := it.checkUsable(); err != nil {
		return nil, err
	}
	if !it.valid {
		return nil, ErrKeyNotFound
	}
	if it.curPending != nil {
		return it.curPending.Value, nil
	}
	return it.tx.db.getValueByRecord(it.curRecord)
}

// Release closes the underlying index cursor. It is idempotent and never
// fails; it may be called after the owning transaction has ended.
func (it *MergeIterator) Release() {
	if it.released {
		return
	}
	it.releaseCursor()
	it.released = true
	it.valid = false
	it.curKey = nil
	it.curPending = nil
	it.curRecord = nil
	it.iItem = nil
	it.pOK = false
	it.iOK = false
}

// releaseCursor releases the tidwall iterator exactly once.
func (it *MergeIterator) releaseCursor() {
	if it.hasIndex {
		it.indexCursor.Release()
		it.hasIndex = false
	}
}

// checkUsable validates the iterator state without dereferencing a cleaned-up
// database.
func (it *MergeIterator) checkUsable() error {
	if it.released {
		return ErrIteratorReleased
	}
	if it.tx == nil || it.tx.isClosed() || it.tx.db == nil {
		return ErrTxClosed
	}
	if it.tx.writeVersion != it.snapshotVersion {
		return ErrMergeIteratorStale
	}
	return nil
}

// cmp compares two keys with the configured comparator and returns -1, 0 or 1.
func (it *MergeIterator) cmp(a, b []byte) int {
	if it.less(a, b) {
		return -1
	}
	if it.less(b, a) {
		return 1
	}
	return 0
}

// inRange reports whether key belongs to the configured [Start, End] interval
// with its open/closed flags.
func (it *MergeIterator) inRange(key []byte) bool {
	if it.options.Start != nil {
		c := it.cmp(key, it.options.Start)
		if c < 0 || (c == 0 && !it.options.StartInclusive) {
			return false
		}
	}
	if it.options.End != nil {
		c := it.cmp(key, it.options.End)
		if c > 0 || (c == 0 && !it.options.EndInclusive) {
			return false
		}
	}
	return true
}

// prefixOK applies the byte-only prefix filter. It must never be used to stop
// a scan early.
func (it *MergeIterator) prefixOK(key []byte) bool {
	return len(it.options.Prefix) == 0 || bytes.HasPrefix(key, it.options.Prefix)
}

// --- committed index cursor primitives ---

func (it *MergeIterator) trackIndex(item *core.Item[core.Record]) {
	if item != nil {
		it.indexVisitedKeys = append(it.indexVisitedKeys, item.Key)
	}
}

func (it *MergeIterator) indexFirst() bool {
	if !it.hasIndex {
		it.iOK, it.iItem = false, nil
		return false
	}
	it.iOK = it.indexCursor.First()
	if it.iOK {
		it.iItem = it.indexCursor.Item()
		it.trackIndex(it.iItem)
	} else {
		it.iItem = nil
	}
	return it.iOK
}

func (it *MergeIterator) indexLast() bool {
	if !it.hasIndex {
		it.iOK, it.iItem = false, nil
		return false
	}
	it.iOK = it.indexCursor.Last()
	if it.iOK {
		it.iItem = it.indexCursor.Item()
		it.trackIndex(it.iItem)
	} else {
		it.iItem = nil
	}
	return it.iOK
}

func (it *MergeIterator) indexNext() bool {
	if !it.hasIndex {
		it.iOK, it.iItem = false, nil
		return false
	}
	it.iOK = it.indexCursor.Next()
	if it.iOK {
		it.iItem = it.indexCursor.Item()
		it.trackIndex(it.iItem)
	} else {
		it.iItem = nil
	}
	return it.iOK
}

func (it *MergeIterator) indexPrev() bool {
	if !it.hasIndex {
		it.iOK, it.iItem = false, nil
		return false
	}
	it.iOK = it.indexCursor.Prev()
	if it.iOK {
		it.iItem = it.indexCursor.Item()
		it.trackIndex(it.iItem)
	} else {
		it.iItem = nil
	}
	return it.iOK
}

// indexSeekGE positions the index cursor at the first committed item whose key
// is greater than or equal to key under the configured comparator.
func (it *MergeIterator) indexSeekGE(key []byte) bool {
	if !it.hasIndex {
		it.iOK, it.iItem = false, nil
		return false
	}
	it.iOK = it.indexCursor.Seek(&core.Item[core.Record]{Key: key})
	if it.iOK {
		it.iItem = it.indexCursor.Item()
		it.trackIndex(it.iItem)
	} else {
		it.iItem = nil
	}
	return it.iOK
}

// indexSeekLE positions the index cursor at the last committed item whose key
// is less than or equal to key under the configured comparator.
func (it *MergeIterator) indexSeekLE(key []byte) bool {
	if !it.hasIndex {
		it.iOK, it.iItem = false, nil
		return false
	}

	// Seek lands on the first item >= key; step back once when it is strictly
	// greater. When nothing is >= key, start from the last item.
	ok := it.indexCursor.Seek(&core.Item[core.Record]{Key: key})
	if !ok {
		ok = it.indexCursor.Last()
	} else if it.cmp(it.indexCursor.Item().Key, key) > 0 {
		ok = it.indexCursor.Prev()
	}

	it.iOK = ok
	if ok {
		it.iItem = it.indexCursor.Item()
		it.trackIndex(it.iItem)
	} else {
		it.iItem = nil
	}
	return ok
}

// --- positioning and k-way (two-source) merge ---

// positionForward places both cursors at their first candidate not below
// lowerBound (nil means the beginning) and advances to the first visible
// merged item.
func (it *MergeIterator) positionForward(lowerBound []byte) (bool, error) {
	if lowerBound != nil {
		it.indexSeekGE(lowerBound)
		// First pending key >= lowerBound.
		it.pPos = sort.Search(len(it.pending), func(i int) bool {
			return !it.less(it.pending[i].key, lowerBound)
		})
	} else {
		it.indexFirst()
		it.pPos = 0
	}
	it.pOK = it.pPos < len(it.pending)
	return it.advanceForward()
}

// positionReverse places both cursors at their last candidate not above
// upperBound (nil means the end) and advances to the first visible merged item.
func (it *MergeIterator) positionReverse(upperBound []byte) (bool, error) {
	if upperBound != nil {
		it.indexSeekLE(upperBound)
		// First pending key > upperBound, then step back to the last <= bound.
		firstAbove := sort.Search(len(it.pending), func(i int) bool {
			return it.less(upperBound, it.pending[i].key)
		})
		it.pPos = firstAbove - 1
	} else {
		it.indexLast()
		it.pPos = len(it.pending) - 1
	}
	it.pOK = it.pPos >= 0
	return it.advanceReverse()
}

// advanceForward consumes the smallest available head until a visible merged
// item is found or both sources are exhausted.
func (it *MergeIterator) advanceForward() (bool, error) {
	for {
		if !it.pOK && !it.iOK {
			it.invalidateCurrent()
			return false, nil
		}

		choosePending := it.pendingHeadFirst()
		if choosePending {
			p := it.pending[it.pPos]
			it.pPos++
			it.pOK = it.pPos < len(it.pending)

			// On an equal key the pending state dominates and the committed
			// item is consumed in the same step, so a masked old value can
			// never be emitted later.
			if it.iOK && it.cmp(p.key, it.iItem.Key) == 0 {
				it.indexNext()
			}

			// Both sources are sorted ascending: a key beyond End terminates
			// the whole scan.
			if it.options.End != nil && it.cmp(p.key, it.options.End) > 0 {
				it.iOK = false
				it.invalidateCurrent()
				return false, nil
			}

			if p.deleted || p.expired {
				continue
			}
			if !it.inRange(p.key) || !it.prefixOK(p.key) {
				continue
			}
			it.setCurrentPending(p)
			return true, nil
		}

		item := it.iItem
		// Stop before advancing the cursor so the committed index is never
		// walked beyond the configured upper bound.
		if it.options.End != nil && it.cmp(item.Key, it.options.End) > 0 {
			it.pOK = false
			it.invalidateCurrent()
			return false, nil
		}
		it.indexNext()

		if it.isIndexItemExpired(item) {
			continue
		}
		if !it.inRange(item.Key) || !it.prefixOK(item.Key) {
			continue
		}
		it.setCurrentIndex(item)
		return true, nil
	}
}

// advanceReverse consumes the largest available head until a visible merged
// item is found or both sources are exhausted.
func (it *MergeIterator) advanceReverse() (bool, error) {
	for {
		if !it.pOK && !it.iOK {
			it.invalidateCurrent()
			return false, nil
		}

		choosePending := it.pendingHeadLast()
		if choosePending {
			p := it.pending[it.pPos]
			it.pPos--
			it.pOK = it.pPos >= 0

			if it.iOK && it.cmp(p.key, it.iItem.Key) == 0 {
				it.indexPrev()
			}

			// Both sources are sorted descending here: a key below Start
			// terminates the whole scan.
			if it.options.Start != nil && it.cmp(p.key, it.options.Start) < 0 {
				it.iOK = false
				it.invalidateCurrent()
				return false, nil
			}

			if p.deleted || p.expired {
				continue
			}
			if !it.inRange(p.key) || !it.prefixOK(p.key) {
				continue
			}
			it.setCurrentPending(p)
			return true, nil
		}

		item := it.iItem
		// Stop before advancing the cursor so the committed index is never
		// walked below the configured lower bound.
		if it.options.Start != nil && it.cmp(item.Key, it.options.Start) < 0 {
			it.pOK = false
			it.invalidateCurrent()
			return false, nil
		}
		it.indexPrev()

		if it.isIndexItemExpired(item) {
			continue
		}
		if !it.inRange(item.Key) || !it.prefixOK(item.Key) {
			continue
		}
		it.setCurrentIndex(item)
		return true, nil
	}
}

// pendingHeadFirst reports whether the pending head must be consumed before
// the index head in ascending order. Pending wins ties because it is the
// newer state.
func (it *MergeIterator) pendingHeadFirst() bool {
	if !it.pOK {
		return false
	}
	if !it.iOK {
		return true
	}
	return it.cmp(it.pending[it.pPos].key, it.iItem.Key) <= 0
}

// pendingHeadLast reports whether the pending head must be consumed before the
// index head in descending order. Pending wins ties.
func (it *MergeIterator) pendingHeadLast() bool {
	if !it.pOK {
		return false
	}
	if !it.iOK {
		return true
	}
	return it.cmp(it.pending[it.pPos].key, it.iItem.Key) >= 0
}

func (it *MergeIterator) isIndexItemExpired(item *core.Item[core.Record]) bool {
	if item == nil || item.Record == nil {
		return true
	}
	return it.tx.db.ttlService.IsExpired(item.Record.TTL, item.Record.Timestamp)
}

func (it *MergeIterator) setCurrentPending(p pendingMergeItem) {
	it.valid = true
	it.curKey = p.key
	it.curPending = p.entry
	it.curRecord = nil
}

func (it *MergeIterator) setCurrentIndex(item *core.Item[core.Record]) {
	it.valid = true
	it.curKey = item.Key
	it.curPending = nil
	it.curRecord = item.Record
}

func (it *MergeIterator) invalidateCurrent() {
	it.valid = false
	it.curKey = nil
	it.curPending = nil
	it.curRecord = nil
}
