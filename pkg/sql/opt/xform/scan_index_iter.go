// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package xform

import (
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
)

// indexRejectFlags contains flags designating types of indexes to filter out
// during iteration. For example, the iterator would skip over inverted and
// partial indexes given these flags:
//
//   flags := rejectInvertedIndexes|rejectPartialIndexes
//
type indexRejectFlags int8

const (
	// rejectNoIndexes is the default, which includes all indexes during
	// iteration.
	rejectNoIndexes indexRejectFlags = 0

	// rejectPrimaryIndex excludes the primary index during iteration.
	rejectPrimaryIndex indexRejectFlags = 1 << (iota - 1)

	// rejectInvertedIndexes excludes any inverted indexes during iteration.
	rejectInvertedIndexes

	// rejectNonInvertedIndexes excludes any non-inverted indexes during
	// iteration.
	rejectNonInvertedIndexes

	// rejectPartialIndexes excludes any partial indexes during iteration.
	rejectPartialIndexes

	// rejectNonPartialIndexes excludes any non-partial indexes during
	// iteration.
	rejectNonPartialIndexes
)

// scanIndexIter is a helper struct that supports iteration over the indexes
// of a Scan operator table. For example:
//
//   iter := makeScanIndexIter(mem, scanPrivate, rejectNoIndexes)
//   for iter.Next() {
//     index := iter.Index()
//     cols := iter.IndexColumns()
//     isCovering := iter.IsCovering()
//   }
//
type scanIndexIter struct {
	mem         *memo.Memo
	scanPrivate *memo.ScanPrivate
	tabMeta     *opt.TableMeta
	rejectFlags indexRejectFlags

	// remainingIndexOrds contains the remaining indices in the list
	// of the table's indices. remainingIndexOrds[0] is the index
	// the scanIndexIter currently points at.
	remainingIndexOrds []cat.IndexOrdinal

	// currIndex is the current cat.Index that has been iterated to.
	currIndex cat.Index

	// indexColsCache caches the set of columns included in the index. See
	// IndexColumns for more details.
	indexColsCache opt.ColSet
}

// makeScanIndexIter returns an initialized scanIndexIter.
//
// The rejectFlags determine which types of indexes to skip when iterating.
func makeScanIndexIter(
	mem *memo.Memo, scanPrivate *memo.ScanPrivate, rejectFlags indexRejectFlags,
) scanIndexIter {
	tabMeta := mem.Metadata().TableMeta(scanPrivate.Table)
	iterationOrder := append([]int{-1}, rand.Perm(tabMeta.Table.IndexCount())...)

	return scanIndexIter{
		mem:                mem,
		scanPrivate:        scanPrivate,
		tabMeta:            tabMeta,
		remainingIndexOrds: iterationOrder,
		rejectFlags:        rejectFlags,
	}
}

// Clone creates a new iterator at the same point as the current one. Next will
// thus iterate to the index directly after the one the source iterator currently
// points to.
func (it *scanIndexIter) Clone() scanIndexIter {
	return scanIndexIter{
		mem:                it.mem,
		scanPrivate:        it.scanPrivate,
		tabMeta:            it.tabMeta,
		remainingIndexOrds: it.remainingIndexOrds,
		rejectFlags:        it.rejectFlags,
	}
}

// Next advances iteration to the next index of the Scan operator's table.
// When there are no more indexes to enumerate, next returns false. The
// current index is accessible via the iterator's "index" field.
//
// The rejectFlags set in makeScanIndexIter determine which indexes to skip when
// iterating, if any.
//
// If the ForceIndex flag is set on the scanPrivate, then all indexes except the
// forced index are skipped. Note that the index forced by the ForceIndex flag
// is not guaranteed to be iterated on - it will be skipped if it is rejected by
// the rejectFlags.
func (it *scanIndexIter) Next() bool {
	for {
		it.remainingIndexOrds = it.remainingIndexOrds[1:]

		if len(it.remainingIndexOrds) == 0 {
			it.currIndex = nil
			return false
		}

		indexOrd := it.remainingIndexOrds[0]
		it.currIndex = it.tabMeta.Table.Index(indexOrd)

		// Skip over the primary index if rejectPrimaryIndex is set.
		if it.hasRejectFlag(rejectPrimaryIndex) && indexOrd == cat.PrimaryIndex {
			continue
		}

		// Skip over inverted indexes if rejectInvertedIndexes is set.
		if it.hasRejectFlag(rejectInvertedIndexes) && it.currIndex.IsInverted() {
			continue
		}

		// Skip over non-inverted indexes if rejectNonInvertedIndexes is set.
		if it.hasRejectFlag(rejectNonInvertedIndexes) && !it.currIndex.IsInverted() {
			continue
		}

		if it.hasRejectFlag(rejectPartialIndexes | rejectNonPartialIndexes) {
			_, isPartialIndex := it.currIndex.Predicate()

			// Skip over partial indexes if rejectPartialIndexes is set.
			if it.hasRejectFlag(rejectPartialIndexes) && isPartialIndex {
				continue
			}

			// Skip over non-partial indexes if rejectNonPartialIndexes is set.
			if it.hasRejectFlag(rejectNonPartialIndexes) && !isPartialIndex {
				continue
			}
		}

		// If we are forcing a specific index, ignore all other indexes.
		if it.scanPrivate.Flags.ForceIndex && it.scanPrivate.Flags.Index != indexOrd {
			continue
		}

		// Reset the cols so they can be recalculated.
		it.indexColsCache = opt.ColSet{}
		return true
	}
}

// IndexOrdinal returns the ordinal of the current index that has been iterated
// to.
func (it *scanIndexIter) IndexOrdinal() int {
	return it.remainingIndexOrds[0]
}

// Index returns the current index that has been iterated to.
func (it *scanIndexIter) Index() cat.Index {
	return it.currIndex
}

// IndexColumns returns the set of columns contained in the current index. This
// set includes the columns indexed and stored, as well as the primary key
// columns.
// TODO(mgartner): Caching here will no longer be necessary if we cache in
// TableMeta. See the TODO at TableMeta.IndexColumns.
func (it *scanIndexIter) IndexColumns() opt.ColSet {
	if it.indexColsCache.Empty() {
		it.indexColsCache = it.tabMeta.IndexColumns(it.remainingIndexOrds[0])
	}
	return it.indexColsCache
}

// IsCovering returns true if the current index "covers" all the columns projected
// by the Scan operator. An index covers any columns that it indexes or stores,
// as well as any primary key columns.
func (it *scanIndexIter) IsCovering() bool {
	return it.scanPrivate.Cols.SubsetOf(it.IndexColumns())
}

// hasRejectFlag returns true if the given flag is set in the rejectFlags.
func (it *scanIndexIter) hasRejectFlag(flag indexRejectFlags) bool {
	return it.rejectFlags&flag != 0
}
