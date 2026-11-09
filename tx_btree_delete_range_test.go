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
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/nutsdb/nutsdb/internal/ttl"
	"github.com/stretchr/testify/require"
)

// ---------- helpers ----------

func testOpts(dir string, segmentSize int64) Options {
	opts := DefaultOptions
	opts.Dir = dir
	if segmentSize > 0 {
		opts.SegmentSize = segmentSize
	}
	// Deterministic TTL: disable the timing wheel and drive time via mock clock.
	opts.TTLConfig.EnableTimingWheel = false
	opts.MergeInterval = 0
	return opts
}

func mustOpen(t *testing.T, opts Options) *DB {
	db, err := Open(opts)
	require.NoError(t, err)
	return db
}

func mustCreateKVBucket(t *testing.T, db *DB, bucket string) {
	txCreateBucket(t, db, DataStructureBTree, bucket, nil)
}

func mustPut(t *testing.T, db *DB, bucket string, key, value []byte, ttlSeconds uint32) {
	require.NoError(t, db.Update(func(tx *Tx) error {
		return tx.Put(bucket, key, value, ttlSeconds)
	}))
}

func mustGet(t *testing.T, db *DB, bucket string, key []byte) []byte {
	var value []byte
	require.NoError(t, db.View(func(tx *Tx) error {
		v, err := tx.Get(bucket, key)
		if err != nil {
			return err
		}
		value = v
		return nil
	}))
	return value
}

func mustGetErr(t *testing.T, db *DB, bucket string, key []byte) error {
	return db.View(func(tx *Tx) error {
		_, err := tx.Get(bucket, key)
		return err
	})
}

// dataFileEntryStats scans every data file in dir and counts entries and
// DataDeleteRangeFlag records.
func dataFileEntryStats(t *testing.T, dir string) (totalEntries, rangeEntries int, flags []uint16) {
	userIDs, mergeIDs, err := enumerateDataFileIDs(dir)
	require.NoError(t, err)
	ids := append(append([]int64{}, userIDs...), mergeIDs...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	for _, fid := range ids {
		path := getDataPath(fid, dir)
		fr, err := newFileRecovery(path, 4*KB)
		require.NoError(t, err)

		var off int64
		for {
			entry, rerr := fr.readEntry(off)
			if rerr != nil {
				if errors.Is(rerr, io.EOF) || errors.Is(rerr, ErrIndexOutOfBound) ||
					errors.Is(rerr, io.ErrUnexpectedEOF) || errors.Is(rerr, ErrEntryZero) ||
					errors.Is(rerr, core.ErrHeaderSizeOutOfBounds) {
					break
				}
				require.NoError(t, rerr)
			}
			if entry == nil {
				break
			}
			totalEntries++
			flags = append(flags, entry.Meta.Flag)
			if entry.Meta.Flag == DataDeleteRangeFlag {
				rangeEntries++
			}
			off += entry.Size()
		}
		_ = fr.release()
	}
	return totalEntries, rangeEntries, flags
}

// orderedMapModel is an independent reference model: a plain sorted map that
// applies the same point/range operations in call order.
type orderedMapModel struct {
	values map[string]string
}

func newOrderedMapModel() *orderedMapModel {
	return &orderedMapModel{values: map[string]string{}}
}

func (m *orderedMapModel) put(key, value string) {
	m.values[key] = value
}

func (m *orderedMapModel) delete(key string) {
	delete(m.values, key)
}

func (m *orderedMapModel) deleteRange(start, end string) int {
	removed := 0
	for key := range m.values {
		if key >= start && key < end {
			delete(m.values, key)
			removed++
		}
	}
	return removed
}

func (m *orderedMapModel) snapshot() map[string]string {
	out := make(map[string]string, len(m.values))
	for k, v := range m.values {
		out[k] = v
	}
	return out
}

func assertDBMatchesModel(t *testing.T, db *DB, bucket string, model *orderedMapModel) {
	var keys [][]byte
	var values [][]byte
	require.NoError(t, db.View(func(tx *Tx) error {
		k, v, err := tx.GetAll(bucket)
		if err != nil {
			return err
		}
		keys, values = k, v
		return nil
	}))

	require.Len(t, keys, len(model.values), "visible key count diverges from model")
	got := make(map[string]string, len(keys))
	for i, key := range keys {
		got[string(key)] = string(values[i])
	}
	require.Equal(t, model.snapshot(), got)
}

// ---------- codec / payload validation ----------

func TestDeleteRange_PayloadCodec(t *testing.T) {
	start := []byte("k000")
	end := []byte("k999-with-longer-boundary")

	encoded := core.EncodeRangeDeleteValue(end)
	decoded, err := core.DecodeRangeDeleteValue(encoded)
	require.NoError(t, err)
	require.Equal(t, end, decoded)

	entry := core.NewEntry().
		WithKey(append([]byte(nil), start...)).
		WithValue(encoded).
		WithMeta(core.NewMetaData().
			WithKeySize(uint32(len(start))).
			WithValueSize(uint32(len(encoded))).
			WithFlag(core.DataDeleteRangeFlag).
			WithDs(core.DataStructureBTree))

	gotStart, gotEnd, err := entry.DecodeRangeDelete()
	require.NoError(t, err)
	require.Equal(t, start, gotStart)
	require.Equal(t, end, gotEnd)

	// Full entry encoding keeps CRC protection over the boundary payload.
	raw := entry.Encode()
	parsed := core.NewEntry()
	headerSize, err := parsed.ParseMeta(raw)
	require.NoError(t, err)
	require.NoError(t, parsed.ParsePayload(raw[headerSize:]))
	require.NoError(t, func() error {
		crc := parsed.GetCrc(raw[:headerSize])
		if crc != parsed.Meta.Crc {
			return ErrCrc
		}
		return nil
	}())
	_, _, err = parsed.DecodeRangeDelete()
	require.NoError(t, err)
}

func TestDeleteRange_PayloadCodecRejectsMalformed(t *testing.T) {
	mkEntry := func(key, value []byte, flag uint16) *core.Entry {
		return core.NewEntry().
			WithKey(key).
			WithValue(value).
			WithMeta(core.NewMetaData().
				WithKeySize(uint32(len(key))).
				WithValueSize(uint32(len(value))).
				WithFlag(flag).
				WithDs(core.DataStructureBTree))
	}

	cases := []struct {
		name  string
		entry *core.Entry
	}{
		{"wrong flag", mkEntry([]byte("a"), core.EncodeRangeDeleteValue([]byte("b")), DataSetFlag)},
		{"empty start", mkEntry(nil, core.EncodeRangeDeleteValue([]byte("b")), DataDeleteRangeFlag)},
		{"empty value", mkEntry([]byte("a"), nil, DataDeleteRangeFlag)},
		{"truncated length prefix", mkEntry([]byte("a"), []byte{0x80}, DataDeleteRangeFlag)},
		{"zero end length", mkEntry([]byte("a"), []byte{0}, DataDeleteRangeFlag)},
		{"length larger than payload", mkEntry([]byte("a"), []byte{5, 'x'}, DataDeleteRangeFlag)},
		{"trailing bytes", mkEntry([]byte("a"), append(core.EncodeRangeDeleteValue([]byte("b")), 'x'), DataDeleteRangeFlag)},
		{"end smaller than start", mkEntry([]byte("z"), core.EncodeRangeDeleteValue([]byte("a")), DataDeleteRangeFlag)},
		{"end equal to start", mkEntry([]byte("a"), core.EncodeRangeDeleteValue([]byte("a")), DataDeleteRangeFlag)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := tc.entry.DecodeRangeDelete()
			require.ErrorIs(t, err, core.ErrRangeDeletePayloadInvalid)
		})
	}
}

func TestDeleteRange_CorruptPayloadFailsCRC(t *testing.T) {
	start := []byte("k000")
	end := []byte("k999")
	entry := core.NewEntry().
		WithKey(append([]byte(nil), start...)).
		WithValue(core.EncodeRangeDeleteValue(end)).
		WithMeta(core.NewMetaData().
			WithKeySize(uint32(len(start))).
			WithValueSize(uint32(len(core.EncodeRangeDeleteValue(end)))).
			WithFlag(core.DataDeleteRangeFlag).
			WithDs(core.DataStructureBTree))

	raw := entry.Encode()
	// Flip a byte inside the end boundary payload.
	corrupt := append([]byte(nil), raw...)
	corrupt[len(corrupt)-1] ^= 0xff

	parsed := core.NewEntry()
	headerSize, err := parsed.ParseMeta(corrupt)
	require.NoError(t, err)
	require.NoError(t, parsed.ParsePayload(corrupt[headerSize:]))
	crc := parsed.GetCrc(corrupt[:headerSize])
	require.NotEqual(t, parsed.Meta.Crc, crc)
}

// ---------- validation ----------

func TestDeleteRange_Validation(t *testing.T) {
	withDefaultDB(t, func(t *testing.T, db *DB) {
		bucket := "bucket-validation"
		mustCreateKVBucket(t, db, bucket)
		mustPut(t, db, bucket, []byte("k1"), []byte("v1"), Persistent)

		t.Run("read only tx rejected", func(t *testing.T) {
			tx, err := db.Begin(false)
			require.NoError(t, err)
			n, err := tx.DeleteRange(bucket, []byte("a"), []byte("z"))
			require.ErrorIs(t, err, ErrTxNotWritable)
			require.Zero(t, n)
			require.NoError(t, tx.Rollback())
		})

		t.Run("bucket missing", func(t *testing.T) {
			require.NoError(t, db.Update(func(tx *Tx) error {
				n, err := tx.DeleteRange("no-such-bucket", []byte("a"), []byte("z"))
				require.ErrorIs(t, err, ErrBucketNotExist)
				require.Zero(t, n)
				return nil
			}))
		})

		t.Run("empty boundaries", func(t *testing.T) {
			require.NoError(t, db.Update(func(tx *Tx) error {
				n, err := tx.DeleteRange(bucket, nil, []byte("z"))
				require.ErrorIs(t, err, ErrRangeBoundaryEmpty)
				require.Zero(t, n)

				n, err = tx.DeleteRange(bucket, []byte("a"), []byte{})
				require.ErrorIs(t, err, ErrRangeBoundaryEmpty)
				require.Zero(t, n)
				return nil
			}))
			// No side effect: data is still readable.
			require.Equal(t, []byte("v1"), mustGet(t, db, bucket, []byte("k1")))
		})

		t.Run("reversed interval", func(t *testing.T) {
			require.NoError(t, db.Update(func(tx *Tx) error {
				n, err := tx.DeleteRange(bucket, []byte("z"), []byte("a"))
				require.ErrorIs(t, err, ErrStartGreaterThanEnd)
				require.Zero(t, n)
				return nil
			}))
			require.Equal(t, []byte("v1"), mustGet(t, db, bucket, []byte("k1")))
		})

		t.Run("equal boundaries are side effect free no-op", func(t *testing.T) {
			before, beforeRange, _ := dataFileEntryStats(t, db.opt.Dir)
			require.NoError(t, db.Update(func(tx *Tx) error {
				n, err := tx.DeleteRange(bucket, []byte("k1"), []byte("k1"))
				require.NoError(t, err)
				require.Zero(t, n)
				return nil
			}))
			after, afterRange, _ := dataFileEntryStats(t, db.opt.Dir)
			require.Equal(t, before, after, "equal boundaries must not append a record")
			require.Equal(t, beforeRange, afterRange)
			require.Equal(t, []byte("v1"), mustGet(t, db, bucket, []byte("k1")))
		})
	})
}

// ---------- basic half-open semantics ----------

func TestDeleteRange_BasicHalfOpen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nutsdb-range")
	opts := testOpts(dir, 0)
	db := mustOpen(t, opts)
	defer func() { require.NoError(t, db.Close()) }()

	bucket := "bucket"
	mustCreateKVBucket(t, db, bucket)

	for i := 0; i < 10; i++ {
		mustPut(t, db, bucket, []byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("v%02d", i)), Persistent)
	}

	var deleted int
	require.NoError(t, db.Update(func(tx *Tx) error {
		n, err := tx.DeleteRange(bucket, []byte("k02"), []byte("k05"))
		require.NoError(t, err)
		deleted = n
		return nil
	}))
	// [k02, k05) covers k02, k03, k04.
	require.Equal(t, 3, deleted)

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("k%02d", i))
		err := mustGetErr(t, db, bucket, key)
		if i >= 2 && i < 5 {
			require.ErrorIs(t, err, ErrKeyNotFound, "key %s should be deleted", key)
		} else {
			require.NoError(t, err, "key %s should survive", key)
		}
	}

	// End boundary itself must survive (half-open).
	require.Equal(t, []byte("v05"), mustGet(t, db, bucket, []byte("k05")))
}

// ---------- same-transaction interleaving and relative ordering ----------

func TestDeleteRange_SameTxInterleaving(t *testing.T) {
	withDefaultDB(t, func(t *testing.T, db *DB) {
		bucket := "bucket-interleave"
		mustCreateKVBucket(t, db, bucket)
		mustPut(t, db, bucket, []byte("old1"), []byte("o1"), Persistent)
		mustPut(t, db, bucket, []byte("old2"), []byte("o2"), Persistent)

		t.Run("Put then DeleteRange then Put keeps last value", func(t *testing.T) {
			require.NoError(t, db.Update(func(tx *Tx) error {
				require.NoError(t, tx.Put(bucket, []byte("a"), []byte("v1"), Persistent))

				n, err := tx.DeleteRange(bucket, []byte("a"), []byte("b"))
				require.NoError(t, err)
				// "a" was visible inside the tx, so it counts.
				require.Equal(t, 1, n)

				_, err = tx.Get(bucket, []byte("a"))
				require.ErrorIs(t, err, ErrKeyNotFound)

				require.NoError(t, tx.Put(bucket, []byte("a"), []byte("v2"), Persistent))
				value, err := tx.Get(bucket, []byte("a"))
				require.NoError(t, err)
				require.Equal(t, []byte("v2"), value)
				return nil
			}))
			require.Equal(t, []byte("v2"), mustGet(t, db, bucket, []byte("a")))
		})

		t.Run("DeleteRange then Put then DeleteRange removes it", func(t *testing.T) {
			require.NoError(t, db.Update(func(tx *Tx) error {
				n, err := tx.DeleteRange(bucket, []byte("old1"), []byte("z"))
				require.NoError(t, err)
				require.Equal(t, 2, n) // old1 and old2 were visible

				require.NoError(t, tx.Put(bucket, []byte("p"), []byte("pv"), Persistent))
				value, err := tx.Get(bucket, []byte("p"))
				require.NoError(t, err)
				require.Equal(t, []byte("pv"), value)

				n, err = tx.DeleteRange(bucket, []byte("p"), []byte("q"))
				require.NoError(t, err)
				require.Equal(t, 1, n)

				_, err = tx.Get(bucket, []byte("p"))
				require.ErrorIs(t, err, ErrKeyNotFound)
				return nil
			}))
			require.ErrorIs(t, mustGetErr(t, db, bucket, []byte("p")), ErrKeyNotFound)
			require.ErrorIs(t, mustGetErr(t, db, bucket, []byte("old1")), ErrKeyNotFound)
			require.ErrorIs(t, mustGetErr(t, db, bucket, []byte("old2")), ErrKeyNotFound)
		})
	})
}

func TestDeleteRange_OverlappingRangesFollowCallOrder(t *testing.T) {
	withDefaultDB(t, func(t *testing.T, db *DB) {
		bucket := "bucket-overlap"
		mustCreateKVBucket(t, db, bucket)
		for i := 0; i < 10; i++ {
			mustPut(t, db, bucket, []byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("v%02d", i)), Persistent)
		}

		require.NoError(t, db.Update(func(tx *Tx) error {
			// First range removes k02..k06.
			n, err := tx.DeleteRange(bucket, []byte("k02"), []byte("k07"))
			require.NoError(t, err)
			require.Equal(t, 5, n)

			// Overlapping range: previously removed keys do not count again.
			n, err = tx.DeleteRange(bucket, []byte("k05"), []byte("k09"))
			require.NoError(t, err)
			require.Equal(t, 2, n) // only k07, k08 are still visible

			// A new put inside the second range must survive until another op removes it.
			require.NoError(t, tx.Put(bucket, []byte("k06"), []byte("revived"), Persistent))
			value, err := tx.Get(bucket, []byte("k06"))
			require.NoError(t, err)
			require.Equal(t, []byte("revived"), value)

			n, err = tx.DeleteRange(bucket, []byte("k06"), []byte("k07"))
			require.NoError(t, err)
			require.Equal(t, 1, n)
			_, err = tx.Get(bucket, []byte("k06"))
			require.ErrorIs(t, err, ErrKeyNotFound)
			return nil
		}))

		for i := 0; i < 10; i++ {
			err := mustGetErr(t, db, bucket, []byte(fmt.Sprintf("k%02d", i)))
			if i >= 2 && i < 9 {
				require.ErrorIs(t, err, ErrKeyNotFound)
			} else {
				require.NoError(t, err)
			}
		}
	})
}

func TestDeleteRange_TxVisibility_GetScanHasCount(t *testing.T) {
	withDefaultDB(t, func(t *testing.T, db *DB) {
		bucket := "bucket-visibility"
		mustCreateKVBucket(t, db, bucket)
		for i := 0; i < 6; i++ {
			mustPut(t, db, bucket, []byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("v%02d", i)), Persistent)
		}

		require.NoError(t, db.Update(func(tx *Tx) error {
			n, err := tx.DeleteRange(bucket, []byte("k01"), []byte("k04"))
			require.NoError(t, err)
			require.Equal(t, 3, n)

			// Range delete masks older index items.
			for _, key := range []string{"k01", "k02", "k03"} {
				_, err = tx.Get(bucket, []byte(key))
				require.ErrorIs(t, err, ErrKeyNotFound)
				exists, herr := tx.Has(bucket, []byte(key))
				require.ErrorIs(t, herr, ErrKeyNotFound)
				require.False(t, exists)
			}

			// Items outside the range remain visible.
			value, err := tx.Get(bucket, []byte("k04"))
			require.NoError(t, err)
			require.Equal(t, []byte("v04"), value)

			// Range scan hides masked items.
			keys, values, err := tx.RangeScanEntries(bucket, []byte("k00"), []byte("k05"), true, true)
			require.NoError(t, err)
			require.Equal(t, [][]byte{[]byte("k00"), []byte("k04"), []byte("k05")}, keys)
			require.Equal(t, [][]byte{[]byte("v00"), []byte("v04"), []byte("v05")}, values)

			// A later put inside the deleted range is visible.
			require.NoError(t, tx.Put(bucket, []byte("k02"), []byte("new"), Persistent))
			value, err = tx.Get(bucket, []byte("k02"))
			require.NoError(t, err)
			require.Equal(t, []byte("new"), value)

			keys, values, err = tx.RangeScanEntries(bucket, []byte("k00"), []byte("k05"), true, true)
			require.NoError(t, err)
			require.Equal(t, [][]byte{[]byte("k00"), []byte("k02"), []byte("k04"), []byte("k05")}, keys)
			require.Equal(t, [][]byte{[]byte("v00"), []byte("new"), []byte("v04"), []byte("v05")}, values)

			// Another range delete removes the revived key too.
			n, err = tx.DeleteRange(bucket, []byte("k02"), []byte("k03"))
			require.NoError(t, err)
			require.Equal(t, 1, n)
			_, err = tx.Get(bucket, []byte("k02"))
			require.ErrorIs(t, err, ErrKeyNotFound)
			return nil
		}))
	})
}

func TestDeleteRange_RollbackHasNoEffect(t *testing.T) {
	withDefaultDB(t, func(t *testing.T, db *DB) {
		bucket := "bucket-rollback"
		mustCreateKVBucket(t, db, bucket)
		for i := 0; i < 5; i++ {
			mustPut(t, db, bucket, []byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("v%02d", i)), Persistent)
		}

		tx, err := db.Begin(true)
		require.NoError(t, err)
		n, err := tx.DeleteRange(bucket, []byte("k00"), []byte("k04"))
		require.NoError(t, err)
		require.Equal(t, 4, n)
		require.NoError(t, tx.Rollback())

		for i := 0; i < 5; i++ {
			require.Equal(t, []byte(fmt.Sprintf("v%02d", i)),
				mustGet(t, db, bucket, []byte(fmt.Sprintf("k%02d", i))))
		}
	})
}

// ---------- TTL ----------

func TestDeleteRange_TTLItems(t *testing.T) {
	runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
		bucket := "bucket-ttl"
		mustCreateKVBucket(t, db, bucket)

		mustPut(t, db, bucket, []byte("k01"), []byte("v01"), 10)
		mustPut(t, db, bucket, []byte("k02"), []byte("v02"), Persistent)
		mustPut(t, db, bucket, []byte("k03"), []byte("v03"), 10)
		mustPut(t, db, bucket, []byte("k04"), []byte("v04"), Persistent)

		// Expire k01 and k03.
		mc.AdvanceTime(11 * time.Second)

		// Only live items count and get removed.
		require.NoError(t, db.Update(func(tx *Tx) error {
			n, err := tx.DeleteRange(bucket, []byte("k00"), []byte("k05"))
			require.NoError(t, err)
			require.Equal(t, 2, n, "expired items must not be counted as visible")
			return nil
		}))

		require.ErrorIs(t, mustGetErr(t, db, bucket, []byte("k02")), ErrKeyNotFound)
		require.ErrorIs(t, mustGetErr(t, db, bucket, []byte("k04")), ErrKeyNotFound)
	})
}

// ---------- multiple buckets ----------

func TestDeleteRange_MultipleBuckets(t *testing.T) {
	withDefaultDB(t, func(t *testing.T, db *DB) {
		b1, b2 := "bucket-a", "bucket-b"
		mustCreateKVBucket(t, db, b1)
		mustCreateKVBucket(t, db, b2)

		for _, bucket := range []string{b1, b2} {
			for i := 0; i < 5; i++ {
				mustPut(t, db, bucket, []byte(fmt.Sprintf("k%02d", i)), []byte(bucket), Persistent)
			}
		}

		require.NoError(t, db.Update(func(tx *Tx) error {
			n, err := tx.DeleteRange(b1, []byte("k00"), []byte("k03"))
			require.NoError(t, err)
			require.Equal(t, 3, n)
			return nil
		}))

		for i := 0; i < 5; i++ {
			key := []byte(fmt.Sprintf("k%02d", i))
			if i < 3 {
				require.ErrorIs(t, mustGetErr(t, db, b1, key), ErrKeyNotFound)
			} else {
				require.NoError(t, mustGetErr(t, db, b1, key))
			}
			// b2 is completely untouched.
			require.NoError(t, mustGetErr(t, db, b2, key))
		}
	})
}

// ---------- single range record on disk ----------

func TestDeleteRange_AppendsSingleLogRecord(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nutsdb-log")
	opts := testOpts(dir, 0)
	db := mustOpen(t, opts)

	bucket := "bucket-log"
	mustCreateKVBucket(t, db, bucket)

	const n = 50
	for i := 0; i < n; i++ {
		mustPut(t, db, bucket, []byte(fmt.Sprintf("k%04d", i)), []byte(fmt.Sprintf("v%04d", i)), Persistent)
	}

	beforeTotal, beforeRange, _ := dataFileEntryStats(t, dir)
	require.Zero(t, beforeRange)

	require.NoError(t, db.Update(func(tx *Tx) error {
		deleted, err := tx.DeleteRange(bucket, []byte("k0000"), []byte("k0100"))
		require.NoError(t, err)
		require.Equal(t, n, deleted)
		return nil
	}))

	afterTotal, afterRange, flags := dataFileEntryStats(t, dir)
	require.Equal(t, beforeTotal+1, afterTotal,
		"deleting %d keys must append exactly one record", n)
	require.Equal(t, 1, afterRange)

	// The appended record really carries the range delete flag.
	require.Equal(t, uint16(DataDeleteRangeFlag), flags[len(flags)-1])

	// A second, zero-match call still persists one ordered tombstone.
	require.NoError(t, db.Update(func(tx *Tx) error {
		deleted, err := tx.DeleteRange(bucket, []byte("k0000"), []byte("k0100"))
		require.NoError(t, err)
		require.Zero(t, deleted)
		return nil
	}))
	_, afterRange2, _ := dataFileEntryStats(t, dir)
	require.Equal(t, 2, afterRange2)

	require.NoError(t, db.Close())
}

// ---------- forced small segment files (rotation) and reopen ----------

func TestDeleteRange_SmallSegmentsAndReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nutsdb-small-seg")
	opts := testOpts(dir, 1024) // force frequent rotation
	db := mustOpen(t, opts)

	bucket := "bucket-small"
	mustCreateKVBucket(t, db, bucket)

	for i := 0; i < 40; i++ {
		mustPut(t, db, bucket, []byte(fmt.Sprintf("k%04d", i)), make([]byte, 64), Persistent)
	}

	require.NoError(t, db.Update(func(tx *Tx) error {
		n, err := tx.DeleteRange(bucket, []byte("k0010"), []byte("k0030"))
		require.NoError(t, err)
		require.Equal(t, 20, n)
		return nil
	}))

	userIDs, _, err := enumerateDataFileIDs(dir)
	require.NoError(t, err)
	require.Greater(t, len(userIDs), 1, "small segment size must force multiple data files")

	require.NoError(t, db.Close())

	db = mustOpen(t, opts)
	defer func() { require.NoError(t, db.Close()) }()

	for i := 0; i < 40; i++ {
		key := []byte(fmt.Sprintf("k%04d", i))
		err := mustGetErr(t, db, bucket, key)
		if i >= 10 && i < 30 {
			require.ErrorIs(t, err, ErrKeyNotFound)
		} else {
			require.NoError(t, err)
		}
	}
}

// ---------- randomized model comparison across reopen ----------

func TestDeleteRange_MatchesOrderedMapModel(t *testing.T) {
	for _, idxMode := range []EntryIdxMode{HintKeyValAndRAMIdxMode, HintKeyAndRAMIdxMode} {
		t.Run(fmt.Sprintf("idxMode=%d", idxMode), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "nutsdb-model")
			opts := testOpts(dir, 4096)
			opts.EntryIdxMode = idxMode
			db := mustOpen(t, opts)

			bucket := "bucket-model"
			mustCreateKVBucket(t, db, bucket)
			model := newOrderedMapModel()

			rng := rand.New(rand.NewSource(42))
			keyAt := func(i int) string { return fmt.Sprintf("k%04d", i) }

			for round := 0; round < 60; round++ {
				require.NoError(t, db.Update(func(tx *Tx) error {
					// Each transaction performs a small random interleaved batch,
					// so point and range ops share one commit and one ordering.
					for step := 0; step < 6; step++ {
						switch rng.Intn(4) {
						case 0, 1:
							i := rng.Intn(60)
							key := keyAt(i)
							value := fmt.Sprintf("v-%d-%d", round, step)
							if err := tx.Put(bucket, []byte(key), []byte(value), Persistent); err != nil {
								return err
							}
							model.put(key, value)
						case 2:
							i := rng.Intn(60)
							key := keyAt(i)
							if err := tx.Delete(bucket, []byte(key)); err != nil &&
								!errors.Is(err, ErrKeyNotFound) {
								return err
							}
							model.delete(key)
						case 3:
							lo := rng.Intn(60)
							hi := lo + rng.Intn(10)
							start, end := keyAt(lo), keyAt(hi)
							n, err := tx.DeleteRange(bucket, []byte(start), []byte(end))
							if err != nil {
								return err
							}
							require.Equal(t, model.deleteRange(start, end), n,
								"round %d step %d range [%s,%s) count mismatch", round, step, start, end)
						}
					}
					return nil
				}))
			}

			assertDBMatchesModel(t, db, bucket, model)
			require.NoError(t, db.Close())

			// Reopen and compare again: recovery must reproduce the same state.
			db = mustOpen(t, opts)
			assertDBMatchesModel(t, db, bucket, model)
			require.NoError(t, db.Close())
		})
	}
}

// ---------- truncated / interrupted commit recovery ----------

func TestDeleteRange_TruncatedRecovery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nutsdb-truncate")
	opts := testOpts(dir, 256*KB)
	db := mustOpen(t, opts)

	bucket := "bucket-trunc"
	mustCreateKVBucket(t, db, bucket)

	// Transaction 1: baseline keys.
	for i := 0; i < 10; i++ {
		mustPut(t, db, bucket, []byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("v%02d", i)), Persistent)
	}

	// Transaction 2: extra puts and a range delete sharing one commit marker.
	require.NoError(t, db.Update(func(tx *Tx) error {
		for i := 10; i < 20; i++ {
			if err := tx.Put(bucket, []byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("v%02d", i)), Persistent); err != nil {
				return err
			}
		}
		_, err := tx.DeleteRange(bucket, []byte("k00"), []byte("k05"))
		return err
	}))
	require.NoError(t, db.Close())

	userIDs, _, err := enumerateDataFileIDs(dir)
	require.NoError(t, err)
	lastFID := userIDs[len(userIDs)-1]
	lastPath := getDataPath(lastFID, dir)

	// Find the start offset of the final record (which carries Committed).
	fr, err := newFileRecovery(lastPath, 4*KB)
	require.NoError(t, err)
	var entryOffsets []int64
	var off int64
	for {
		entry, rerr := fr.readEntry(off)
		if rerr != nil {
			break
		}
		if entry == nil {
			break
		}
		entryOffsets = append(entryOffsets, off)
		off += entry.Size()
	}
	require.NoError(t, fr.release())
	require.NotEmpty(t, entryOffsets)

	// Remove the final record together with its commit marker. The whole
	// transaction must be discarded, never applied partially.
	require.NoError(t, os.Truncate(lastPath, entryOffsets[len(entryOffsets)-1]))

	db = mustOpen(t, opts)
	defer func() { require.NoError(t, db.Close()) }()

	// tx2 keys must be entirely absent ...
	for i := 10; i < 20; i++ {
		require.ErrorIs(t, mustGetErr(t, db, bucket, []byte(fmt.Sprintf("k%02d", i))), ErrKeyNotFound)
	}
	// ... and the range delete from tx2 must not have been applied either.
	for i := 0; i < 5; i++ {
		require.NoError(t, mustGetErr(t, db, bucket, []byte(fmt.Sprintf("k%02d", i))),
			"half range delete must never become visible")
	}
	for i := 5; i < 10; i++ {
		require.NoError(t, mustGetErr(t, db, bucket, []byte(fmt.Sprintf("k%02d", i))))
	}
}

// ---------- malformed range record is never indexed ----------

func TestDeleteRange_MalformedRecordNotIndexed(t *testing.T) {
	withDefaultDB(t, func(t *testing.T, db *DB) {
		bucket := "bucket-bad-record"
		mustCreateKVBucket(t, db, bucket)
		b, err := db.bucketMgr.GetBucket(DataStructureBTree, bucket)
		require.NoError(t, err)

		bad := core.NewEntry().
			WithKey([]byte("a")).
			WithValue([]byte{9, 'x'}). // claims 9 end bytes, only provides 1
			WithMeta(core.NewMetaData().
				WithKeySize(1).
				WithValueSize(2).
				WithFlag(DataDeleteRangeFlag).
				WithDs(DataStructureBTree).
				WithBucketId(b.Id))

		require.ErrorIs(t, db.buildBTreeIdx(nil, bad), core.ErrRangeDeletePayloadInvalid)

		idx, ok := db.Index.BTree.exist(b.Id)
		require.True(t, ok)
		require.Zero(t, idx.Count(), "malformed range record must not create an index item")
	})
}

// ---------- merge: no resurrection, new puts survive, hint consistent ----------

func TestDeleteRange_MergeBothPaths(t *testing.T) {
	modes := []struct {
		name          string
		enableMergeV2 bool
	}{
		{"legacy", false},
		{"v2", true},
	}

	for _, mode := range modes {
		for _, idxMode := range []EntryIdxMode{HintKeyValAndRAMIdxMode, HintKeyAndRAMIdxMode} {
			for _, enableHint := range []bool{false, true} {
				name := fmt.Sprintf("%s/idx%d/hint%t", mode.name, idxMode, enableHint)
				t.Run(name, func(t *testing.T) {
					dir := filepath.Join(t.TempDir(), "nutsdb-merge")
					opts := testOpts(dir, KB)
					opts.EnableMergeV2 = mode.enableMergeV2
					opts.EntryIdxMode = idxMode
					opts.EnableHintFile = enableHint

					db := mustOpen(t, opts)
					bucket := "bucket-merge"
					mustCreateKVBucket(t, db, bucket)

					const total = 200
					for i := 0; i < total; i++ {
						mustPut(t, db, bucket, []byte(fmt.Sprintf("k%04d", i)),
							[]byte(fmt.Sprintf("v%04d", i)), Persistent)
					}

					// Range delete removes k0010..k0029.
					require.NoError(t, db.Update(func(tx *Tx) error {
						n, err := tx.DeleteRange(bucket, []byte("k0010"), []byte("k0030"))
						require.NoError(t, err)
						require.Equal(t, 20, n)
						return nil
					}))

					// A new put inside the deleted range after the range delete.
					mustPut(t, db, bucket, []byte("k0015"), []byte("fresh"), Persistent)

					require.NoError(t, db.Merge())

					// Deleted values must not resurrect, the fresh put must survive.
					for i := 0; i < total; i++ {
						key := []byte(fmt.Sprintf("k%04d", i))
						err := mustGetErr(t, db, bucket, key)
						switch {
						case i == 15:
							require.NoError(t, err)
							require.Equal(t, []byte("fresh"), mustGet(t, db, bucket, key))
						case i >= 10 && i < 30:
							require.ErrorIs(t, err, ErrKeyNotFound)
						default:
							require.NoError(t, err)
						}
					}

					require.NoError(t, db.Close())

					// Reopen: hint-based and full-scan recovery must agree.
					db = mustOpen(t, opts)
					for i := 0; i < total; i++ {
						key := []byte(fmt.Sprintf("k%04d", i))
						err := mustGetErr(t, db, bucket, key)
						switch {
						case i == 15:
							require.NoError(t, err)
							require.Equal(t, []byte("fresh"), mustGet(t, db, bucket, key))
						case i >= 10 && i < 30:
							require.ErrorIs(t, err, ErrKeyNotFound)
						default:
							require.NoError(t, err)
						}
					}
					require.NoError(t, db.Close())
				})
			}
		}
	}
}

// ---------- merge with randomized model ----------

func TestDeleteRange_MergeMatchesModel(t *testing.T) {
	modes := []struct {
		name          string
		enableMergeV2 bool
	}{
		{"legacy", false},
		{"v2", true},
	}

	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "nutsdb-merge-model")
			opts := testOpts(dir, 2*KB)
			opts.EnableMergeV2 = mode.enableMergeV2
			db := mustOpen(t, opts)

			bucket := "bucket"
			mustCreateKVBucket(t, db, bucket)
			model := newOrderedMapModel()
			rng := rand.New(rand.NewSource(7))
			keyAt := func(i int) string { return fmt.Sprintf("k%04d", i) }

			for round := 0; round < 30; round++ {
				require.NoError(t, db.Update(func(tx *Tx) error {
					for step := 0; step < 5; step++ {
						switch rng.Intn(3) {
						case 0:
							i := rng.Intn(80)
							key, value := keyAt(i), fmt.Sprintf("v%d", round*10+step)
							if err := tx.Put(bucket, []byte(key), []byte(value), Persistent); err != nil {
								return err
							}
							model.put(key, value)
						case 1:
							lo, hi := rng.Intn(80), rng.Intn(80)
							if lo > hi {
								lo, hi = hi, lo
							}
							if lo == hi {
								continue
							}
							start, end := keyAt(lo), keyAt(hi)
							n, err := tx.DeleteRange(bucket, []byte(start), []byte(end))
							if err != nil {
								return err
							}
							require.Equal(t, model.deleteRange(start, end), n)
						case 2:
							i := rng.Intn(80)
							key := keyAt(i)
							if err := tx.Delete(bucket, []byte(key)); err != nil &&
								!errors.Is(err, ErrKeyNotFound) {
								return err
							}
							model.delete(key)
						}
					}
					return nil
				}))
			}

			assertDBMatchesModel(t, db, bucket, model)
			require.NoError(t, db.Merge())
			assertDBMatchesModel(t, db, bucket, model)
			require.NoError(t, db.Close())

			db = mustOpen(t, opts)
			assertDBMatchesModel(t, db, bucket, model)
			require.NoError(t, db.Close())
		})
	}
}
