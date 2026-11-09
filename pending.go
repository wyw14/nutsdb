package nutsdb

import (
	"bytes"
	"sort"

	"github.com/nutsdb/nutsdb/internal/core"
)

// EntryStatus represents the Entry status in the current Tx
type EntryStatus = uint8

const (
	// NotFoundEntry means there is no changes for this entry in current Tx
	NotFoundEntry EntryStatus = 0
	// EntryDeleted means this Entry has been deleted in the current Tx
	EntryDeleted EntryStatus = 1
	// EntryUpdated means this Entry has been updated in the current Tx
	EntryUpdated EntryStatus = 2
)

// BucketStatus represents the current status of bucket in current Tx
type BucketStatus = uint8

const (
	// BucketStatusExistAlready means this bucket already exists
	BucketStatusExistAlready = 1
	// BucketStatusDeleted means this bucket is already deleted
	BucketStatusDeleted = 2
	// BucketStatusNew means this bucket is created in current Tx
	BucketStatusNew = 3
	// BucketStatusUpdated means this bucket is updated in current Tx
	BucketStatusUpdated = 4
	// BucketStatusUnknown means this bucket doesn't exist
	BucketStatusUnknown = 5
)

// pendingBucketList the uncommitted bucket changes in this Tx
type pendingBucketList map[core.Ds]map[core.BucketName]*core.Bucket

// pendingEntriesInBTree means the point changes (Put/Delete) Entries in
// DataStructureBTree in the Tx, keyed by user key. Only the latest point
// operation per key is retained, exactly like the historical behavior.
type pendingEntriesInBTree map[core.BucketName]map[string]*core.Entry

// pendingEntryList the uncommitted Entry changes in this Tx
type pendingEntryList struct {
	entriesInBTree pendingEntriesInBTree
	// bTreeOps keeps every BTree operation (point operations and range
	// deletes) in exact call order per bucket. Point entries share their
	// pointers with entriesInBTree; superseded point entries stay in the log
	// but are skipped when collecting the commit list. Range delete entries
	// never enter the point map because they are not keyed user values.
	bTreeOps map[core.BucketName][]*core.Entry
	// rangeDeleteCount records the number of currently visible items removed
	// by each range delete operation at the time it was issued.
	rangeDeleteCount map[*core.Entry]int
	entries          map[core.Ds]map[core.BucketName][]*core.Entry
	size             int
}

// newPendingEntriesList create a new pendingEntryList object for a Tx
func newPendingEntriesList() *pendingEntryList {
	pending := &pendingEntryList{
		entriesInBTree:   map[core.BucketName]map[string]*core.Entry{},
		bTreeOps:         map[core.BucketName][]*core.Entry{},
		rangeDeleteCount: map[*core.Entry]int{},
		entries:          map[core.Ds]map[core.BucketName][]*core.Entry{},
		size:             0,
	}
	return pending
}

// submitEntry submit an entry into pendingEntryList
func (pending *pendingEntryList) submitEntry(ds core.Ds, bucket string, e *core.Entry) {
	switch ds {
	case DataStructureBTree:
		if _, exist := pending.entriesInBTree[bucket]; !exist {
			pending.entriesInBTree[bucket] = map[string]*core.Entry{}
		}
		if _, exist := pending.entriesInBTree[bucket][string(e.Key)]; !exist {
			pending.size++
		}
		pending.entriesInBTree[bucket][string(e.Key)] = e
		pending.bTreeOps[bucket] = append(pending.bTreeOps[bucket], e)
	default:
		if _, exist := pending.entries[ds]; !exist {
			pending.entries[ds] = map[core.BucketName][]*core.Entry{}
		}
		entries := pending.entries[ds][bucket]
		entries = append(entries, e)
		pending.entries[ds][bucket] = entries
		pending.size++
	}
}

// submitBTreeRangeEntry appends a BTree range delete entry to the ordered
// operation log. Range deletes are never stored as point values.
func (pending *pendingEntryList) submitBTreeRangeEntry(bucket string, e *core.Entry, deletedCount int) {
	pending.bTreeOps[bucket] = append(pending.bTreeOps[bucket], e)
	pending.rangeDeleteCount[e] = deletedCount
	pending.size++
}

// getRangeDeleteCount returns the visible item count removed by the given
// range delete entry when it was issued.
func (pending *pendingEntryList) getRangeDeleteCount(e *core.Entry) int {
	return pending.rangeDeleteCount[e]
}

func (pending *pendingEntryList) Get(ds core.Ds, bucket string, key []byte) (entry *core.Entry, err error) {
	switch ds {
	case DataStructureBTree:
		if _, exist := pending.entriesInBTree[bucket]; exist {
			if rec, ok := pending.entriesInBTree[bucket][string(key)]; ok {
				return rec, nil
			} else {
				return nil, ErrKeyNotFound
			}
		}
		return nil, ErrBucketNotFound
	default:
		if _, exist := pending.entries[ds]; exist {
			if entries, ok := pending.entries[ds][bucket]; ok {
				for _, e := range entries {
					if bytes.Equal(key, e.Key) {
						return e, nil
					}
				}
				return nil, ErrKeyNotFound
			} else {
				return nil, ErrKeyNotFound
			}
		}
		return nil, ErrBucketNotFound
	}
}

func (pending *pendingEntryList) GetTTL(ds core.Ds, bucket string, key []byte) (ttl int64, err error) {
	rec, err := pending.Get(ds, bucket, key)
	if err != nil {
		return 0, err
	}
	if rec.Meta.TTL == Persistent {
		return -1, nil
	}
	return int64(expireTime(rec.Meta.Timestamp, rec.Meta.TTL).Seconds()), nil
}

// keyInHalfOpenRange reports whether key belongs to [start, end) under the
// database key order (lexicographic byte order).
func keyInHalfOpenRange(key, start, end []byte) bool {
	return bytes.Compare(key, start) >= 0 && bytes.Compare(key, end) < 0
}

// keyInClosedRange reports whether key belongs to [start, end]. It preserves
// the historical inclusive RangeScan boundary semantics.
func keyInClosedRange(key, start, end []byte) bool {
	return bytes.Compare(key, start) >= 0 && bytes.Compare(key, end) <= 0
}

// bTreePendingView is the effective BTree pending state of one bucket after
// replaying its point and range operations in call order.
type bTreePendingView struct {
	// lastPoint holds the newest point operation per key that a later range
	// delete has not shadowed. Delete-flag entries remain as tombstones.
	lastPoint map[string]*core.Entry
	// ranges holds the decoded [start, end) boundaries of all range deletes
	// in call order.
	ranges [][2][]byte
}

// buildBTreeView replays the ordered BTree operation log of one bucket.
func (pending *pendingEntryList) buildBTreeView(bucket core.BucketName) *bTreePendingView {
	view := &bTreePendingView{lastPoint: map[string]*core.Entry{}}

	for _, op := range pending.bTreeOps[bucket] {
		if op.Meta.Flag == DataDeleteRangeFlag {
			start, end, err := op.DecodeRangeDelete()
			if err != nil {
				// Entries are validated on submission; never expect this.
				continue
			}
			// A range delete shadows every earlier pending point in range.
			for key := range view.lastPoint {
				if keyInHalfOpenRange([]byte(key), start, end) {
					delete(view.lastPoint, key)
				}
			}
			view.ranges = append(view.ranges, [2][]byte{start, end})
			continue
		}
		view.lastPoint[string(op.Key)] = op
	}

	return view
}

// status returns the effective pending status of one key.
func (view *bTreePendingView) status(key []byte) (EntryStatus, *core.Entry) {
	if entry, ok := view.lastPoint[string(key)]; ok {
		if entry.Meta.Flag == DataDeleteFlag {
			return EntryDeleted, nil
		}
		return EntryUpdated, entry
	}

	for _, boundaries := range view.ranges {
		if keyInHalfOpenRange(key, boundaries[0], boundaries[1]) {
			return EntryDeleted, nil
		}
	}

	return NotFoundEntry, nil
}

// isRangeMasked reports whether an older index item for key must be hidden
// because a pending range delete covers it (and no later point op revived it).
func (view *bTreePendingView) isRangeMasked(key []byte) bool {
	if _, ok := view.lastPoint[string(key)]; ok {
		return false
	}
	for _, boundaries := range view.ranges {
		if keyInHalfOpenRange(key, boundaries[0], boundaries[1]) {
			return true
		}
	}
	return false
}

// visiblePuts returns the effective pending point puts within the requested
// boundaries, sorted by key. Tombstones and range-shadowed keys are excluded.
func (view *bTreePendingView) visiblePuts(start, end []byte, inclusiveEnd bool) (keys, values [][]byte) {
	inRange := keyInHalfOpenRange
	if inclusiveEnd {
		inRange = keyInClosedRange
	}

	keys = make([][]byte, 0)
	values = make([][]byte, 0)
	for _, entry := range view.lastPoint {
		if entry.Meta.Flag == DataDeleteFlag {
			continue
		}
		if inRange(entry.Key, start, end) {
			keys = append(keys, entry.Key)
			values = append(values, entry.Value)
		}
	}
	sort.Sort(&sortkv{
		k: keys,
		v: values,
	})
	return keys, values
}

// bTreeStatus is a thin pending-side wrapper used by Tx helpers.
func (pending *pendingEntryList) bTreeStatus(bucket core.BucketName, key []byte) (EntryStatus, *core.Entry) {
	if len(pending.bTreeOps[bucket]) == 0 {
		return NotFoundEntry, nil
	}
	return pending.buildBTreeView(bucket).status(key)
}

// rangeBucket input a range handler function f and call it with every bucket in pendingBucketList
func (p pendingBucketList) rangeBucket(f func(bucket *core.Bucket) error) error {
	for _, bucketsInDs := range p {
		for _, bucket := range bucketsInDs {
			err := f(bucket)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// toList collect all the entries in pendingEntryList to a list.
// BTree entries are emitted per bucket in exact operation order: range
// deletes keep their position, while superseded point entries are skipped and
// only the latest point entry per key is emitted.
func (pending *pendingEntryList) toList() []*core.Entry {
	list := make([]*core.Entry, 0, pending.size)
	for bucket, ops := range pending.bTreeOps {
		latestPoints := pending.entriesInBTree[bucket]
		for _, entry := range ops {
			if entry.Meta.Flag == DataDeleteRangeFlag {
				list = append(list, entry)
				continue
			}
			if latestPoints[string(entry.Key)] == entry {
				list = append(list, entry)
			}
		}
	}
	for _, entriesInDS := range pending.entries {
		for _, entries := range entriesInDS {
			list = append(list, entries...)
		}
	}
	return list
}

// MaxOrMinKey returns the max/min key among the effective pending point puts
// of a bucket. Keys shadowed by a later range delete are excluded.
func (pending *pendingEntryList) MaxOrMinKey(bucketName string, isMax bool) (key []byte, found bool) {
	if len(pending.bTreeOps[bucketName]) == 0 {
		return nil, false
	}

	view := pending.buildBTreeView(bucketName)
	var maxKey, minKey []byte
	pendingFound := false
	for _, entry := range view.lastPoint {
		if entry.Meta.Flag == DataDeleteFlag {
			continue
		}
		maxKey = compareAndReturn(maxKey, entry.Key, 1)
		minKey = compareAndReturn(minKey, entry.Key, -1)
		pendingFound = true
	}

	if !pendingFound {
		return nil, false
	}
	if isMax {
		return maxKey, true
	}
	return minKey, true
}

// isBucketNotFoundStatus return true for bucket is not found,
// false for other status.
func isBucketNotFoundStatus(status BucketStatus) bool {
	return status == BucketStatusDeleted || status == BucketStatusUnknown
}
