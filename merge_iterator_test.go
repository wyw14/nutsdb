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
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/nutsdb/nutsdb/internal/ttl"
	"github.com/stretchr/testify/require"
)

var mergeIteratorIdxModes = []struct {
	name string
	mode EntryIdxMode
}{
	{"RAM", HintKeyValAndRAMIdxMode},
	{"Hint", HintKeyAndRAMIdxMode},
}

func mergeIteratorBaseOpts(mode EntryIdxMode, less LessFunc) Options {
	opts := DefaultOptions
	opts.EntryIdxMode = mode
	opts.SyncEnable = false
	opts.LessFunc = less
	return opts
}

// lengthThenLexLess is a deliberately non-lexicographic comparator: keys are
// ordered by length first, then lexicographically. Byte-prefix matched keys
// are therefore not contiguous, which exercises the "prefix must not stop a
// scan early" requirement.
func lengthThenLexLess(l, r string) bool {
	if len(l) != len(r) {
		return len(l) < len(r)
	}
	return l < r
}

func testCmp(less func(a, b []byte) bool, a, b []byte) int {
	if less(a, b) {
		return -1
	}
	if less(b, a) {
		return 1
	}
	return 0
}

func collectMergeIterator(t *testing.T, it *MergeIterator) (keys, values [][]byte) {
	t.Helper()
	for it.Valid() {
		k, err := it.Key()
		require.NoError(t, err)
		v, err := it.Value()
		require.NoError(t, err)
		keys = append(keys, append([]byte(nil), k...))
		values = append(values, append([]byte(nil), v...))
		ok, err := it.Next()
		require.NoError(t, err)
		if !ok {
			break
		}
	}
	return keys, values
}

// oracleInRange mirrors MergeIterator.inRange using the test comparator.
func oracleInRange(less func(a, b []byte) bool, key []byte, opts MergeIteratorOptions) bool {
	if opts.Start != nil {
		c := testCmp(less, key, opts.Start)
		if c < 0 || (c == 0 && !opts.StartInclusive) {
			return false
		}
	}
	if opts.End != nil {
		c := testCmp(less, key, opts.End)
		if c > 0 || (c == 0 && !opts.EndInclusive) {
			return false
		}
	}
	return true
}

// expectedView builds the reference result from an independent ordered map:
// filter by bounds and byte prefix, sort with the comparator and reverse when
// requested.
func expectedView(t *testing.T, model map[string][]byte, less func(a, b []byte) bool, opts MergeIteratorOptions) (keys, values [][]byte) {
	t.Helper()
	sorted := make([][]byte, 0, len(model))
	for k := range model {
		sorted = append(sorted, []byte(k))
	}
	sort.Slice(sorted, func(i, j int) bool { return less(sorted[i], sorted[j]) })

	for _, k := range sorted {
		if !oracleInRange(less, k, opts) {
			continue
		}
		if len(opts.Prefix) > 0 && !bytes.HasPrefix(k, opts.Prefix) {
			continue
		}
		keys = append(keys, append([]byte(nil), k...))
		values = append(values, append([]byte(nil), model[string(k)]...))
	}
	if opts.Reverse {
		for i, j := 0, len(keys)-1; i < j; i, j = i+1, j-1 {
			keys[i], keys[j] = keys[j], keys[i]
			values[i], values[j] = values[j], values[i]
		}
	}
	return keys, values
}

func commitKV(t *testing.T, db *DB, bucket string, kvs map[string]string) {
	t.Helper()
	require.NoError(t, db.Update(func(tx *Tx) error {
		for k, v := range kvs {
			require.NoError(t, tx.Put(bucket, []byte(k), []byte(v), Persistent))
		}
		return nil
	}))
}

func copyModel(src map[string]string) map[string][]byte {
	dst := make(map[string][]byte, len(src))
	for k, v := range src {
		dst[k] = []byte(v)
	}
	return dst
}

// TestMergeIterator_BasicMerge exercises override, delete and new pending keys
// in both directions and both index modes, against an independent map model.
func TestMergeIterator_BasicMerge(t *testing.T) {
	bucket := "bucket"
	committed := map[string]string{"a": "A", "b": "B", "c": "C", "e": "E"}

	for _, tc := range mergeIteratorIdxModes {
		t.Run(tc.name, func(t *testing.T) {
			runNutsDBTest(t, mergeIteratorOptsPtr(tc.mode, nil), func(t *testing.T, db *DB) {
				txCreateBucket(t, db, DataStructureBTree, bucket, nil)
				commitKV(t, db, bucket, committed)

				require.NoError(t, db.Update(func(tx *Tx) error {
					require.NoError(t, tx.Put(bucket, []byte("b"), []byte("B2"), Persistent)) // override
					require.NoError(t, tx.Delete(bucket, []byte("c")))                       // pending delete
					require.NoError(t, tx.Put(bucket, []byte("d"), []byte("D"), Persistent))  // new pending key

					model := copyModel(committed)
					model["b"] = []byte("B2")
					delete(model, "c")
					model["d"] = []byte("D")

					for _, reverse := range []bool{false, true} {
						it, err := NewMergeIterator(tx, bucket, MergeIteratorOptions{Reverse: reverse})
						require.NoError(t, err)
						gotKeys, gotValues := collectMergeIterator(t, it)
						wantKeys, wantValues := expectedView(t, model, bytesLess, MergeIteratorOptions{Reverse: reverse})
						require.Equal(t, wantKeys, gotKeys)
						require.Equal(t, wantValues, gotValues)
						it.Release()
					}
					return nil
				}))
			})
		})
	}
}

func mergeIteratorOptsPtr(mode EntryIdxMode, less LessFunc) *Options {
	opts := mergeIteratorBaseOpts(mode, less)
	return &opts
}

func bytesLess(a, b []byte) bool { return bytes.Compare(a, b) < 0 }

// TestMergeIterator_SameKeyOverride ensures the newest pending value wins and
// the old committed value is never emitted.
func TestMergeIterator_SameKeyOverride(t *testing.T) {
	bucket := "bucket"
	for _, tc := range mergeIteratorIdxModes {
		t.Run(tc.name, func(t *testing.T) {
			runNutsDBTest(t, mergeIteratorOptsPtr(tc.mode, nil), func(t *testing.T, db *DB) {
				txCreateBucket(t, db, DataStructureBTree, bucket, nil)
				commitKV(t, db, bucket, map[string]string{"k": "old"})

				require.NoError(t, db.Update(func(tx *Tx) error {
					require.NoError(t, tx.Put(bucket, []byte("k"), []byte("new"), Persistent))
					it, err := NewMergeIterator(tx, bucket, MergeIteratorOptions{})
					require.NoError(t, err)
					keys, values := collectMergeIterator(t, it)
					require.Equal(t, [][]byte{[]byte("k")}, keys)
					require.Equal(t, [][]byte{[]byte("new")}, values)
					it.Release()

					// Overwriting again inside the same tx still yields the latest value.
					require.NoError(t, tx.Put(bucket, []byte("k"), []byte("newer"), Persistent))
					it2, err := NewMergeIterator(tx, bucket, MergeIteratorOptions{})
					require.NoError(t, err)
					keys, values = collectMergeIterator(t, it2)
					require.Equal(t, [][]byte{[]byte("k")}, keys)
					require.Equal(t, [][]byte{[]byte("newer")}, values)
					it2.Release()
					return nil
				}))
			})
		})
	}
}

// TestMergeIterator_PendingDeleteMasks verifies a pending delete (and a delete
// followed by no re-put) hides the committed value.
func TestMergeIterator_PendingDeleteMasks(t *testing.T) {
	bucket := "bucket"
	for _, tc := range mergeIteratorIdxModes {
		t.Run(tc.name, func(t *testing.T) {
			runNutsDBTest(t, mergeIteratorOptsPtr(tc.mode, nil), func(t *testing.T, db *DB) {
				txCreateBucket(t, db, DataStructureBTree, bucket, nil)
				commitKV(t, db, bucket, map[string]string{"k1": "v1", "k2": "v2", "k3": "v3"})

				require.NoError(t, db.Update(func(tx *Tx) error {
					require.NoError(t, tx.Delete(bucket, []byte("k2")))
					it, err := NewMergeIterator(tx, bucket, MergeIteratorOptions{})
					require.NoError(t, err)
					keys, _ := collectMergeIterator(t, it)
					require.Equal(t, [][]byte{[]byte("k1"), []byte("k3")}, keys)
					it.Release()
					return nil
				}))
			})
		})
	}
}

// TestMergeIterator_ExpiredPendingMasks checks that an expired pending value
// masks the older committed value instead of letting it re-appear, and that
// expired committed records are filtered too.
func TestMergeIterator_ExpiredPendingMasks(t *testing.T) {
	bucket := "bucket"
	for _, tc := range mergeIteratorIdxModes {
		t.Run(tc.name, func(t *testing.T) {
			opts := mergeIteratorBaseOpts(tc.mode, nil)
			runNutsDBTestWithMockClock(t, &opts, func(t *testing.T, db *DB, mc ttl.Clock) {
				txCreateBucket(t, db, DataStructureBTree, bucket, nil)
				// k1 persistent, k2 committed with a short TTL, k3 persistent.
				require.NoError(t, db.Update(func(tx *Tx) error {
					require.NoError(t, tx.Put(bucket, []byte("k1"), []byte("v1"), Persistent))
					require.NoError(t, tx.Put(bucket, []byte("k2"), []byte("v2"), 1))
					require.NoError(t, tx.Put(bucket, []byte("k3"), []byte("v3"), Persistent))
					return nil
				}))

				mc.AdvanceTime(2 * time.Second)

				require.NoError(t, db.Update(func(tx *Tx) error {
					// Expired pending write must tombstone the committed k1 value.
					require.NoError(t, tx.Put(bucket, []byte("k1"), []byte("v1-expired"), 1))
					mc.AdvanceTime(2 * time.Second)

					it, err := NewMergeIterator(tx, bucket, MergeIteratorOptions{})
					require.NoError(t, err)
					keys, values := collectMergeIterator(t, it)
					require.Equal(t, [][]byte{[]byte("k3")}, keys)
					require.Equal(t, [][]byte{[]byte("v3")}, values)
					it.Release()
					return nil
				}))
			})
		})
	}
}

// TestMergeIterator_Bounds covers independent open/closed start/end bounds and
// pending keys landing both inside and outside the range.
func TestMergeIterator_Bounds(t *testing.T) {
	bucket := "bucket"
	for _, tc := range mergeIteratorIdxModes {
		t.Run(tc.name, func(t *testing.T) {
			runNutsDBTest(t, mergeIteratorOptsPtr(tc.mode, nil), func(t *testing.T, db *DB) {
				txCreateBucket(t, db, DataStructureBTree, bucket, nil)
				committed := map[string]string{}
				for i := 0; i < 10; i++ {
					k := fmt.Sprintf("k%d", i)
					committed[k] = fmt.Sprintf("v%d", i)
				}
				commitKV(t, db, bucket, committed)

				require.NoError(t, db.Update(func(tx *Tx) error {
					// Pending keys below, inside and above the range.
					require.NoError(t, tx.Put(bucket, []byte("k0"), []byte("below"), Persistent))
					require.NoError(t, tx.Put(bucket, []byte("k5"), []byte("inside"), Persistent))
					require.NoError(t, tx.Delete(bucket, []byte("k3")))
					require.NoError(t, tx.Put(bucket, []byte("k9"), []byte("above"), Persistent))

					model := copyModel(committed)
					model["k0"] = []byte("below")
					model["k5"] = []byte("inside")
					delete(model, "k3")
					model["k9"] = []byte("above")

					opts := MergeIteratorOptions{
						Start:          []byte("k2"),
						End:            []byte("k7"),
						StartInclusive: true,
						EndInclusive:   false,
					}
					it, err := NewMergeIterator(tx, bucket, opts)
					require.NoError(t, err)
					gotKeys, gotValues := collectMergeIterator(t, it)
					wantKeys, wantValues := expectedView(t, model, bytesLess, opts)
					require.Equal(t, wantKeys, gotKeys)
					require.Equal(t, wantValues, gotValues)
					require.Equal(t, [][]byte{
						[]byte("k2"), []byte("k4"), []byte("k5"), []byte("k6"),
					}, gotKeys)

					// The index cursor must stay within the requested range apart
					// from at most the single stop probe beyond End.
					beyond := 0
					for _, visited := range it.indexVisitedKeys {
						if testCmp(bytesLess, visited, opts.Start) < 0 {
							t.Fatalf("index cursor visited %s below Start %s", visited, opts.Start)
						}
						if testCmp(bytesLess, visited, opts.End) > 0 {
							beyond++
						}
					}
					require.LessOrEqual(t, beyond, 1)
					it.Release()

					// Single-key boundary combinations.
					cases := []struct {
						name        string
						startIncl   bool
						endIncl     bool
						expectCount int
					}{
						{"both closed", true, true, 1},
						{"open start", false, true, 0},
						{"open end", true, false, 0},
						{"both open", false, false, 0},
					}
					for _, c := range cases {
						t.Run(c.name, func(t *testing.T) {
							o := MergeIteratorOptions{
								Start:          []byte("k4"),
								End:            []byte("k4"),
								StartInclusive: c.startIncl,
								EndInclusive:   c.endIncl,
							}
							it, err := NewMergeIterator(tx, bucket, o)
							require.NoError(t, err)
							keys, _ := collectMergeIterator(t, it)
							require.Len(t, keys, c.expectCount)
							it.Release()
						})
					}
					return nil
				}))
			})
		})
	}
}

// TestMergeIterator_Seek verifies forward GE seek and reverse LE seek together
// with range intersection and continued traversal.
func TestMergeIterator_Seek(t *testing.T) {
	bucket := "bucket"
	for _, tc := range mergeIteratorIdxModes {
		t.Run(tc.name, func(t *testing.T) {
			runNutsDBTest(t, mergeIteratorOptsPtr(tc.mode, nil), func(t *testing.T, db *DB) {
				txCreateBucket(t, db, DataStructureBTree, bucket, nil)
				committed := map[string]string{}
				for _, i := range []int{0, 2, 4, 6, 8} {
					committed[fmt.Sprintf("k%d", i)] = fmt.Sprintf("v%d", i)
				}
				commitKV(t, db, bucket, committed)

				require.NoError(t, db.Update(func(tx *Tx) error {
					for _, i := range []int{3, 5, 7} {
						require.NoError(t, tx.Put(bucket, []byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("p%d", i)), Persistent))
					}

					// Ascending: first item >= target.
					it, err := NewMergeIterator(tx, bucket, MergeIteratorOptions{})
					require.NoError(t, err)
					ok, err := it.Seek([]byte("k3"))
					require.NoError(t, err)
					require.True(t, ok)
					k, err := it.Key()
					require.NoError(t, err)
					require.Equal(t, []byte("k3"), k)
					ok, err = it.Next()
					require.NoError(t, err)
					require.True(t, ok)
					k, _ = it.Key()
					require.Equal(t, []byte("k4"), k)

					ok, err = it.Seek([]byte("k9"))
					require.NoError(t, err)
					require.False(t, ok)
					require.False(t, it.Valid())
					it.Release()

					// Ascending seek intersected with an active range.
					it, err = NewMergeIterator(tx, bucket, MergeIteratorOptions{
						Start: []byte("k2"), End: []byte("k6"),
						StartInclusive: true, EndInclusive: true,
					})
					require.NoError(t, err)
					ok, err = it.Seek([]byte("k0"))
					require.NoError(t, err)
					require.True(t, ok)
					k, _ = it.Key()
					require.Equal(t, []byte("k2"), k)
					ok, err = it.Seek([]byte("k7"))
					require.NoError(t, err)
					require.False(t, ok)
					it.Release()

					// Descending: first item <= target, then keep moving down.
					it, err = NewMergeIterator(tx, bucket, MergeIteratorOptions{Reverse: true})
					require.NoError(t, err)
					ok, err = it.Seek([]byte("k5"))
					require.NoError(t, err)
					require.True(t, ok)
					k, _ = it.Key()
					require.Equal(t, []byte("k5"), k)
					ok, err = it.Next()
					require.NoError(t, err)
					require.True(t, ok)
					k, _ = it.Key()
					require.Equal(t, []byte("k4"), k)

					// Reverse seek clamped by the range upper bound.
					it.Release()
					it, err = NewMergeIterator(tx, bucket, MergeIteratorOptions{
						Reverse: true,
						Start:   []byte("k2"), End: []byte("k6"),
						StartInclusive: true, EndInclusive: true,
					})
					require.NoError(t, err)
					ok, err = it.Seek([]byte("k9"))
					require.NoError(t, err)
					require.True(t, ok)
					k, _ = it.Key()
					require.Equal(t, []byte("k6"), k)
					ok, err = it.Seek([]byte("k1"))
					require.NoError(t, err)
					require.False(t, ok)
					it.Release()
					return nil
				}))
			})
		})
	}
}

// TestMergeIterator_Rewind checks Rewind in both directions after Seek/Next.
func TestMergeIterator_Rewind(t *testing.T) {
	bucket := "bucket"
	for _, tc := range mergeIteratorIdxModes {
		t.Run(tc.name, func(t *testing.T) {
			runNutsDBTest(t, mergeIteratorOptsPtr(tc.mode, nil), func(t *testing.T, db *DB) {
				txCreateBucket(t, db, DataStructureBTree, bucket, nil)
				commitKV(t, db, bucket, map[string]string{
					"k1": "v1", "k2": "v2", "k3": "v3",
				})
				require.NoError(t, db.Update(func(tx *Tx) error {
					require.NoError(t, tx.Put(bucket, []byte("k0"), []byte("v0"), Persistent))

					it, err := NewMergeIterator(tx, bucket, MergeIteratorOptions{})
					require.NoError(t, err)
					_, err = it.Seek([]byte("k3"))
					require.NoError(t, err)
					ok, err := it.Rewind()
					require.NoError(t, err)
					require.True(t, ok)
					k, _ := it.Key()
					require.Equal(t, []byte("k0"), k)
					it.Release()

					rit, err := NewMergeIterator(tx, bucket, MergeIteratorOptions{Reverse: true})
					require.NoError(t, err)
					_, err = rit.Seek([]byte("k1"))
					require.NoError(t, err)
					ok, err = rit.Rewind()
					require.NoError(t, err)
					require.True(t, ok)
					k, _ = rit.Key()
					require.Equal(t, []byte("k3"), k)
					rit.Release()
					return nil
				}))
			})
		})
	}
}

// TestMergeIterator_PrefixAllFF uses an all-0xff prefix under both the default
// and a non-lexicographic comparator, in both directions and index modes.
func TestMergeIterator_PrefixAllFF(t *testing.T) {
	bucket := "bucket"
	keys := [][]byte{
		{0xff},
		{0xff, 0x00},
		{0xff, 0xff},
		{0xff, 0xaa},
		{0x00, 0xff},
		[]byte("abc"),
	}
	comparators := []struct {
		name string
		less LessFunc
	}{
		{"lex", nil},
		{"lengthThenLex", lengthThenLexLess},
	}

	for _, tc := range mergeIteratorIdxModes {
		for _, cmp := range comparators {
			t.Run(tc.name+"_"+cmp.name, func(t *testing.T) {
				runNutsDBTest(t, mergeIteratorOptsPtr(tc.mode, cmp.less), func(t *testing.T, db *DB) {
					txCreateBucket(t, db, DataStructureBTree, bucket, nil)
					require.NoError(t, db.Update(func(tx *Tx) error {
						for _, k := range keys {
							require.NoError(t, tx.Put(bucket, k, append([]byte("v-"), k...), Persistent))
						}
						return nil
					}))

					less := bytesLess
					if cmp.less != nil {
						less = func(a, b []byte) bool { return cmp.less(string(a), string(b)) }
					}
					model := map[string][]byte{}
					for _, k := range keys {
						model[string(k)] = append([]byte("v-"), k...)
					}

					require.NoError(t, db.View(func(tx *Tx) error {
						for _, reverse := range []bool{false, true} {
							opts := MergeIteratorOptions{Reverse: reverse, Prefix: []byte{0xff}}
							it, err := NewMergeIterator(tx, bucket, opts)
							require.NoError(t, err)
							gotKeys, gotValues := collectMergeIterator(t, it)
							wantKeys, wantValues := expectedView(t, model, less, opts)
							require.Equal(t, wantKeys, gotKeys)
							require.Equal(t, wantValues, gotValues)
							require.Len(t, gotKeys, 4)
							it.Release()
						}
						return nil
					}))
				})
			})
		}
	}
}

// TestMergeIterator_CustomLessFunc validates ordering, bounds and seeks with a
// database-configured non-lexicographic comparator.
func TestMergeIterator_CustomLessFunc(t *testing.T) {
	bucket := "bucket"
	runNutsDBTest(t, mergeIteratorOptsPtr(HintKeyValAndRAMIdxMode, lengthThenLexLess), func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
		committed := map[string]string{
			"a": "1", "bb": "2", "ccc": "3", "dddd": "4",
		}
		commitKV(t, db, bucket, committed)
		less := func(a, b []byte) bool { return lengthThenLexLess(string(a), string(b)) }

		require.NoError(t, db.Update(func(tx *Tx) error {
			require.NoError(t, tx.Put(bucket, []byte("ee"), []byte("5"), Persistent)) // length 2
			require.NoError(t, tx.Delete(bucket, []byte("bb")))

			model := map[string][]byte{
				"a": []byte("1"), "ccc": []byte("3"), "dddd": []byte("4"), "ee": []byte("5"),
			}

			// Ascending full scan follows length then lexicographic order:
			// length 1 ("a"), length 2 ("ee"), length 3 ("ccc"), length 4.
			it, err := NewMergeIterator(tx, bucket, MergeIteratorOptions{})
			require.NoError(t, err)
			keys, values := collectMergeIterator(t, it)
			wantKeys, wantValues := expectedView(t, model, less, MergeIteratorOptions{})
			require.Equal(t, wantKeys, keys)
			require.Equal(t, wantValues, values)
			require.Equal(t, [][]byte{
				[]byte("a"), []byte("ee"), []byte("ccc"), []byte("dddd"),
			}, keys)
			it.Release()

			// Bounds are evaluated with the custom comparator.
			opts := MergeIteratorOptions{
				Start:          []byte("b"),
				End:            []byte("ccc"),
				StartInclusive: true,
				EndInclusive:   true,
			}
			it, err = NewMergeIterator(tx, bucket, opts)
			require.NoError(t, err)
			keys, _ = collectMergeIterator(t, it)
			wantKeys, _ = expectedView(t, model, less, opts)
			require.Equal(t, wantKeys, keys)
			// "a" < "b", "ee" has length 2 inside the window, "ccc" is the
			// inclusive end and "dddd" (length 4) is above it.
			require.Equal(t, [][]byte{[]byte("ee"), []byte("ccc")}, keys)
			it.Release()

			// GE/LE seeks with the custom comparator.
			it, err = NewMergeIterator(tx, bucket, MergeIteratorOptions{})
			require.NoError(t, err)
			ok, err := it.Seek([]byte("d"))
			require.NoError(t, err)
			require.True(t, ok)
			k, _ := it.Key()
			require.Equal(t, []byte("ee"), k) // length-1 "a" is below "d"; first length-2 key wins
			it.Release()

			rit, err := NewMergeIterator(tx, bucket, MergeIteratorOptions{Reverse: true})
			require.NoError(t, err)
			ok, err = rit.Seek([]byte("cc"))
			require.NoError(t, err)
			require.True(t, ok)
			k, _ = rit.Key()
			require.Equal(t, []byte("a"), k) // length-1 "a" is the greatest key <= length-2 "cc"
			rit.Release()
			return nil
		}))
	})
}

// TestMergeIterator_EmptyRange covers inverted bounds and fully empty views.
func TestMergeIterator_EmptyRange(t *testing.T) {
	bucket := "bucket"
	for _, tc := range mergeIteratorIdxModes {
		t.Run(tc.name, func(t *testing.T) {
			runNutsDBTest(t, mergeIteratorOptsPtr(tc.mode, nil), func(t *testing.T, db *DB) {
				txCreateBucket(t, db, DataStructureBTree, bucket, nil)
				commitKV(t, db, bucket, map[string]string{"k1": "v1", "k2": "v2", "k3": "v3"})

				require.NoError(t, db.View(func(tx *Tx) error {
					// Inverted range: Start greater than End.
					it, err := NewMergeIterator(tx, bucket, MergeIteratorOptions{
						Start: []byte("k9"), End: []byte("k0"),
						StartInclusive: true, EndInclusive: true,
					})
					require.NoError(t, err)
					require.False(t, it.Valid())
					ok, err := it.Next()
					require.NoError(t, err)
					require.False(t, ok)
					it.Release()

					// Prefix that no key has.
					it, err = NewMergeIterator(tx, bucket, MergeIteratorOptions{Prefix: []byte("zzz")})
					require.NoError(t, err)
					keys, _ := collectMergeIterator(t, it)
					require.Empty(t, keys)
					it.Release()
					return nil
				}))
			})
		})
	}
}

// TestMergeIterator_NewBucket traverses a bucket created in the same
// transaction, including an empty one, and rejects missing/deleted buckets.
func TestMergeIterator_NewBucket(t *testing.T) {
	for _, tc := range mergeIteratorIdxModes {
		t.Run(tc.name, func(t *testing.T) {
			runNutsDBTest(t, mergeIteratorOptsPtr(tc.mode, nil), func(t *testing.T, db *DB) {
				require.NoError(t, db.Update(func(tx *Tx) error {
					require.NoError(t, tx.NewKVBucket("fresh"))

					// Empty brand-new bucket.
					it, err := NewMergeIterator(tx, "fresh", MergeIteratorOptions{})
					require.NoError(t, err)
					require.False(t, it.Valid())
					keys, _ := collectMergeIterator(t, it)
					require.Empty(t, keys)
					it.Release()

					require.NoError(t, tx.Put("fresh", []byte("k1"), []byte("v1"), Persistent))
					require.NoError(t, tx.Put("fresh", []byte("k2"), []byte("v2"), Persistent))

					it, err = NewMergeIterator(tx, "fresh", MergeIteratorOptions{})
					require.NoError(t, err)
					keys, values := collectMergeIterator(t, it)
					require.Equal(t, [][]byte{[]byte("k1"), []byte("k2")}, keys)
					require.Equal(t, [][]byte{[]byte("v1"), []byte("v2")}, values)
					it.Release()

					// Unknown bucket.
					_, err = NewMergeIterator(tx, "missing", MergeIteratorOptions{})
					require.ErrorIs(t, err, ErrNotFoundBucket)
					return nil
				}))

				// Bucket deleted inside the transaction is not traversable.
				txCreateBucket(t, db, DataStructureBTree, "doomed", nil)
				require.NoError(t, db.Update(func(tx *Tx) error {
					require.NoError(t, tx.DeleteBucket(DataStructureBTree, "doomed"))
					_, err := NewMergeIterator(tx, "doomed", MergeIteratorOptions{})
					require.ErrorIs(t, err, ErrNotFoundBucket)
					return nil
				}))
			})
		})
	}
}

// TestMergeIterator_Staleness ensures any new key or bucket write after
// creation invalidates the iterator with an explicit error.
func TestMergeIterator_Staleness(t *testing.T) {
	bucket := "bucket"
	runNutsDBTest(t, mergeIteratorOptsPtr(HintKeyValAndRAMIdxMode, nil), func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
		commitKV(t, db, bucket, map[string]string{"k1": "v1"})

		require.NoError(t, db.Update(func(tx *Tx) error {
			it, err := NewMergeIterator(tx, bucket, MergeIteratorOptions{})
			require.NoError(t, err)
			require.True(t, it.Valid())

			// A new key write bumps the transaction write version.
			require.NoError(t, tx.Put(bucket, []byte("k2"), []byte("v2"), Persistent))
			_, err = it.Next()
			require.ErrorIs(t, err, ErrMergeIteratorStale)
			_, err = it.Rewind()
			require.ErrorIs(t, err, ErrMergeIteratorStale)
			_, err = it.Seek([]byte("k1"))
			require.ErrorIs(t, err, ErrMergeIteratorStale)
			_, err = it.Key()
			require.ErrorIs(t, err, ErrMergeIteratorStale)
			_, err = it.Value()
			require.ErrorIs(t, err, ErrMergeIteratorStale)
			// Release stays usable and idempotent even when stale.
			it.Release()
			it.Release()
			return nil
		}))

		// A bucket write also invalidates.
		require.NoError(t, db.Update(func(tx *Tx) error {
			it, err := NewMergeIterator(tx, bucket, MergeIteratorOptions{})
			require.NoError(t, err)
			require.NoError(t, tx.NewKVBucket("other-bucket"))
			_, err = it.Next()
			require.ErrorIs(t, err, ErrMergeIteratorStale)
			it.Release()
			return nil
		}))
	})
}

// TestMergeIterator_ClosedTx verifies post-commit/rollback access fails without
// touching the cleaned-up database, while Release remains safe.
func TestMergeIterator_ClosedTx(t *testing.T) {
	bucket := "bucket"
	runNutsDBTest(t, mergeIteratorOptsPtr(HintKeyValAndRAMIdxMode, nil), func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
		commitKV(t, db, bucket, map[string]string{"k1": "v1"})

		// After Commit.
		tx, err := db.Begin(true)
		require.NoError(t, err)
		it, err := NewMergeIterator(tx, bucket, MergeIteratorOptions{})
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		_, err = it.Next()
		require.ErrorIs(t, err, ErrTxClosed)
		_, err = it.Key()
		require.ErrorIs(t, err, ErrTxClosed)
		_, err = it.Value()
		require.ErrorIs(t, err, ErrTxClosed)
		it.Release() // must not panic or dereference db

		// After Rollback.
		tx, err = db.Begin(true)
		require.NoError(t, err)
		it, err = NewMergeIterator(tx, bucket, MergeIteratorOptions{})
		require.NoError(t, err)
		require.NoError(t, tx.Rollback())
		_, err = it.Rewind()
		require.ErrorIs(t, err, ErrTxClosed)
		it.Release()

		// Creating an iterator on a closed transaction fails up front.
		tx, err = db.Begin(false)
		require.NoError(t, err)
		require.NoError(t, tx.Rollback())
		_, err = NewMergeIterator(tx, bucket, MergeIteratorOptions{})
		require.ErrorIs(t, err, ErrTxClosed)
	})
}

// TestMergeIterator_Release checks idempotent release and post-release errors.
func TestMergeIterator_Release(t *testing.T) {
	bucket := "bucket"
	runNutsDBTest(t, mergeIteratorOptsPtr(HintKeyAndRAMIdxMode, nil), func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
		commitKV(t, db, bucket, map[string]string{"k1": "v1", "k2": "v2"})

		require.NoError(t, db.View(func(tx *Tx) error {
			it, err := NewMergeIterator(tx, bucket, MergeIteratorOptions{})
			require.NoError(t, err)
			require.True(t, it.hasIndex)
			require.True(t, it.Valid())

			it.Release()
			require.False(t, it.hasIndex)
			require.False(t, it.Valid())

			_, err = it.Next()
			require.ErrorIs(t, err, ErrIteratorReleased)
			_, err = it.Key()
			require.ErrorIs(t, err, ErrIteratorReleased)
			_, err = it.Value()
			require.ErrorIs(t, err, ErrIteratorReleased)
			_, err = it.Seek([]byte("k1"))
			require.ErrorIs(t, err, ErrIteratorReleased)
			_, err = it.Rewind()
			require.ErrorIs(t, err, ErrIteratorReleased)

			// Idempotent: repeated release is a no-op.
			it.Release()
			it.Release()
			return nil
		}))
	})
}

// TestMergeIterator_OnlyScansNecessaryIndexRange proves a narrow scan never
// walks the whole committed tree: the cursor lands only on range keys (plus at
// most one stop probe), regardless of the committed tree size.
func TestMergeIterator_OnlyScansNecessaryIndexRange(t *testing.T) {
	bucket := "bucket"
	const n = 500
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("reverse=%v", reverse), func(t *testing.T) {
			runNutsDBTest(t, mergeIteratorOptsPtr(HintKeyAndRAMIdxMode, nil), func(t *testing.T, db *DB) {
				txCreateBucket(t, db, DataStructureBTree, bucket, nil)
				committed := map[string]string{}
				require.NoError(t, db.Update(func(tx *Tx) error {
					for i := 0; i < n; i++ {
						k := fmt.Sprintf("k%04d", i)
						require.NoError(t, tx.Put(bucket, []byte(k), []byte("v"), Persistent))
						committed[k] = "v"
					}
					return nil
				}))

				require.NoError(t, db.Update(func(tx *Tx) error {
					// A pending tombstone and override inside the narrow window.
					require.NoError(t, tx.Delete(bucket, []byte("k0105")))
					require.NoError(t, tx.Put(bucket, []byte("k0107"), []byte("p"), Persistent))

					opts := MergeIteratorOptions{
						Reverse:        reverse,
						Start:          []byte("k0100"),
						End:            []byte("k0109"),
						StartInclusive: true,
						EndInclusive:   true,
					}
					it, err := NewMergeIterator(tx, bucket, opts)
					require.NoError(t, err)
					keys, _ := collectMergeIterator(t, it)
					require.Len(t, keys, 9) // 10 window keys minus the deleted one
					require.Less(t, len(it.indexVisitedKeys), n/10)

					outOfBounds := 0
					for _, visited := range it.indexVisitedKeys {
						if testCmp(bytesLess, visited, opts.Start) < 0 ||
							testCmp(bytesLess, visited, opts.End) > 0 {
							outOfBounds++
						}
					}
					require.LessOrEqual(t, outOfBounds, 1)
					it.Release()
					return nil
				}))
			})
		})
	}
}

// TestMergeIterator_RandomizedModel fuzzes pending operations, bounds, prefix
// and direction against an independently maintained ordered map in both index
// modes.
func TestMergeIterator_RandomizedModel(t *testing.T) {
	bucket := "bucket"
	const committedCount = 200
	const pendingOps = 40
	seeds := []int64{1, 7, 42, 99}

	for _, tc := range mergeIteratorIdxModes {
		for _, seed := range seeds {
			t.Run(fmt.Sprintf("%s_seed%d", tc.name, seed), func(t *testing.T) {
				runNutsDBTest(t, mergeIteratorOptsPtr(tc.mode, nil), func(t *testing.T, db *DB) {
					txCreateBucket(t, db, DataStructureBTree, bucket, nil)
					committed := map[string]string{}
					require.NoError(t, db.Update(func(tx *Tx) error {
						for i := 0; i < committedCount; i++ {
							k := fmt.Sprintf("k%04d", i)
							v := fmt.Sprintf("v%04d", i)
							committed[k] = v
							require.NoError(t, tx.Put(bucket, []byte(k), []byte(v), Persistent))
						}
						return nil
					}))

					rng := rand.New(rand.NewSource(seed))
					model := map[string][]byte{}
					for k, v := range committed {
						model[k] = []byte(v)
					}

					require.NoError(t, db.Update(func(tx *Tx) error {
						nextNew := committedCount
						for i := 0; i < pendingOps; i++ {
							idx := rng.Intn(committedCount + 20)
							var key string
							if idx < committedCount {
								key = fmt.Sprintf("k%04d", idx)
							} else {
								key = fmt.Sprintf("k%04d", nextNew)
								nextNew++
							}
							switch rng.Intn(3) {
							case 0: // delete, but only when currently visible
								if _, ok := model[key]; !ok {
									continue
								}
								require.NoError(t, tx.Delete(bucket, []byte(key)))
								delete(model, key)
							case 1: // override / insert
								val := fmt.Sprintf("p%04d", i)
								require.NoError(t, tx.Put(bucket, []byte(key), []byte(val), Persistent))
								model[key] = []byte(val)
							default:
								val := fmt.Sprintf("q%04d", i)
								require.NoError(t, tx.Put(bucket, []byte(key), []byte(val), Persistent))
								model[key] = []byte(val)
							}
						}

						prefixes := [][]byte{nil, []byte("k00"), []byte("k01"), []byte("k19"), []byte("zz")}
						prefix := prefixes[rng.Intn(len(prefixes))]

						var start, end []byte
						startIncl, endIncl := rng.Intn(2) == 0, rng.Intn(2) == 0
						switch rng.Intn(3) {
						case 0:
							start = []byte(fmt.Sprintf("k%04d", rng.Intn(committedCount)))
							end = []byte(fmt.Sprintf("k%04d", rng.Intn(committedCount)))
						case 1:
							start = []byte(fmt.Sprintf("k%04d", rng.Intn(committedCount)))
						case 2:
							end = []byte(fmt.Sprintf("k%04d", rng.Intn(committedCount)))
						}

						opts := MergeIteratorOptions{
							Reverse:        rng.Intn(2) == 0,
							Prefix:         prefix,
							Start:          start,
							End:            end,
							StartInclusive: startIncl,
							EndInclusive:   endIncl,
						}
						it, err := NewMergeIterator(tx, bucket, opts)
						require.NoError(t, err)
						gotKeys, gotValues := collectMergeIterator(t, it)
						wantKeys, wantValues := expectedView(t, model, bytesLess, opts)
						require.Equal(t, wantKeys, gotKeys, "key mismatch for opts %+v", opts)
						require.Equal(t, wantValues, gotValues, "value mismatch for opts %+v", opts)
						it.Release()
						return nil
					}))
				})
			})
		}
	}
}

// TestMergeIterator_HintModeLazyValue confirms values are read from disk in
// Hint mode and stay consistent with the RAM mode semantics.
func TestMergeIterator_HintModeLazyValue(t *testing.T) {
	bucket := "bucket"
	runNutsDBTest(t, mergeIteratorOptsPtr(HintKeyAndRAMIdxMode, nil), func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
		commitKV(t, db, bucket, map[string]string{"k1": "disk-value-1", "k2": "disk-value-2"})

		require.NoError(t, db.Update(func(tx *Tx) error {
			require.NoError(t, tx.Put(bucket, []byte("k2"), []byte("pending-value-2"), Persistent))
			it, err := NewMergeIterator(tx, bucket, MergeIteratorOptions{})
			require.NoError(t, err)
			keys, values := collectMergeIterator(t, it)
			require.Equal(t, [][]byte{[]byte("k1"), []byte("k2")}, keys)
			require.Equal(t, [][]byte{[]byte("disk-value-1"), []byte("pending-value-2")}, values)

			// Re-reading a committed value repeatedly works (lazy disk read).
			ok, err := it.Rewind()
			require.NoError(t, err)
			require.True(t, ok)
			v1, err := it.Value()
			require.NoError(t, err)
			require.Equal(t, []byte("disk-value-1"), v1)
			v1Again, err := it.Value()
			require.NoError(t, err)
			require.Equal(t, v1, v1Again)
			it.Release()
			return nil
		}))
	})
}
