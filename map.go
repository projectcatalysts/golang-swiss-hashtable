// Copyright 2023 Dolthub, Inc.
// Copyright 2023 Project Catalysts Ltd.
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

// This version incorporates several small performance optimisations
// introduced by Project Catalysts under the same Apache License, Version 2.0,
// bug fixes and optimisation of the Clear and Iterator method, plus
// functional enhancements including:
// - GetKeys
// - GetValues
// - GetPairs

package swiss

import (
	"github.com/dolthub/maphash"
)

// Map is an open-addressing hash map
// based on Abseil's flat_hash_map.
type Map[K comparable, V any] struct {
	ctrl     []groupMetadata
	groups   []mapGroup[K, V]
	hash     maphash.Hasher[K]
	resident uint32
	dead     uint32
	limit    uint32
}

// MapPair is a key and value pair.
type MapPair[K comparable, V any] struct {
	Key   K
	Value V
}

// mapGroup is a mapGroup of 16 key-value pairs
type mapGroup[K comparable, V any] struct {
	keys   [groupSize]K
	values [groupSize]V
}

// NewMap constructs a Map.
func NewMap[K comparable, V any](sz uint32) (m *Map[K, V]) {
	n := numGroups(sz)
	m = &Map[K, V]{
		ctrl:   make([]groupMetadata, n),
		groups: make([]mapGroup[K, V], n),
		hash:   maphash.NewHasher[K](),
		limit:  n * maxAvgGroupLoad,
	}
	for i := range m.ctrl {
		m.ctrl[i] = emptyMeta
	}
	return
}

// Has returns true if |key| is present in |m|.
func (m *Map[K, V]) Has(key K) (ok bool) {
	var (
		groupCount = uint32(len(m.groups))
		hi, lo     = splitHash(Hash(m.hash.Hash(key)))
		g          = probeStart(hi, groupCount)
		i          uint32
	)
	for { // inlined find loop
		var (
			ctrl    = &m.ctrl[g]
			matches = metaMatchH2(ctrl, lo)
		)
		for matches != 0 {
			i, matches = nextMatch(matches)
			if key == m.groups[g].keys[i] {
				ok = true
				return
			}
		}
		// |key| is not in mapGroup |g|,
		// stop probing if we see an empty slot
		matches = metaMatchEmpty(ctrl)
		if matches != 0 {
			ok = false
			return
		}
		g += 1 // linear probing
		if g >= groupCount {
			g = 0
		}
	}
}

// Get returns the |value| mapped by |key| if one exists.
func (m *Map[K, V]) Get(key K) (value V, ok bool) {
	var (
		groupCount = uint32(len(m.groups))
		hi, lo     = splitHash(Hash(m.hash.Hash(key)))
		g          = probeStart(hi, groupCount)
		i          uint32
	)
	for { // inlined find loop
		ctrl := &m.ctrl[g]
		matches := metaMatchH2(ctrl, lo)
		for matches != 0 {
			group := &m.groups[g]
			i, matches = nextMatch(matches)
			if key == group.keys[i] {
				value, ok = group.values[i], true
				return
			}
		}
		// |key| is not in mapGroup |g|,
		// stop probing if we see an empty slot
		matches = metaMatchEmpty(ctrl)
		if matches != 0 {
			ok = false
			return
		}
		g += 1 // linear probing
		if g >= groupCount {
			g = 0
		}
	}
}

// GetKeys returns the set of all keys.  There will be no duplicates.
func (m *Map[K, V]) GetKeys() []K {
	var (
		groupCount = uint32(len(m.groups))
		keys       = make([]K, m.Count())
		keysIndex  = uint(0)
		g          = randIntN(groupCount) // pick a random starting group
	)
	for n := 0; n < len(m.groups); n++ {
		group := &m.groups[g]
		for i, c := range m.ctrl[g] {
			if c == tombstone {
				continue
			}
			if c == empty {
				// No more hashes in this group
				break
			}
			keys[keysIndex] = group.keys[i]
			keysIndex++
		}
		g++
		if g >= groupCount {
			g = 0
		}
	}

	return keys
}

// GetValues returns the set of all values.  There may be duplicates.
func (m *Map[K, V]) GetValues() []V {
	var (
		groupCount = uint32(len(m.groups))
		values     = make([]V, m.Count())
		valueIndex = uint(0)
		g          = randIntN(groupCount) // pick a random starting group
	)
	for n := 0; n < len(m.groups); n++ {
		group := &m.groups[g]
		for i, c := range m.ctrl[g] {
			if c == tombstone {
				continue
			}
			if c == empty {
				// No more hashes in this group
				break
			}
			values[valueIndex] = group.values[i]
			valueIndex++
		}
		g++
		if g >= groupCount {
			g = 0
		}
	}

	return values
}

// GetPairs returns the set of all hash and value pairs.  Each pair is unique.
func (m *Map[K, V]) GetPairs() []MapPair[K, V] {
	var (
		groupCount = uint32(len(m.groups))
		pairs      = make([]MapPair[K, V], m.Count())
		pairIndex  = uint(0)
		g          = randIntN(groupCount) // pick a random starting group
	)
	for n := 0; n < len(m.groups); n++ {
		group := &m.groups[g]
		for i, c := range m.ctrl[g] {
			if c == tombstone {
				continue
			}
			if c == empty {
				// No more entries in this group
				break
			}
			pairs[pairIndex] = MapPair[K, V]{group.keys[i], group.values[i]}
			pairIndex++
		}
		g++
		if g >= groupCount {
			g = 0
		}
	}

	return pairs
}

// Put attempts to insert |key| and |value|
func (m *Map[K, V]) Put(key K, value V) {
	if m.resident >= m.limit {
		m.rehash(m.nextSize())
	}
	var (
		groupCount = uint32(len(m.groups))
		hi, lo     = splitHash(Hash(m.hash.Hash(key)))
		g          = probeStart(hi, groupCount)
		i          uint32
	)
	for { // inlined find loop
		var (
			ctrl    = &m.ctrl[g]
			group   = &m.groups[g]
			matches = metaMatchH2(ctrl, lo)
		)
		for matches != 0 {
			i, matches = nextMatch(matches)
			if key == group.keys[i] { // update
				group.values[i] = value
				return
			}
		}
		// |key| is not in mapGroup |g|,
		// stop probing if we see an empty slot
		matches = metaMatchEmpty(ctrl)
		if matches != 0 { // insert
			i, _ = nextMatch(matches)
			group.keys[i] = key
			group.values[i] = value
			ctrl[i] = int8(lo)
			m.resident++
			return
		}
		g += 1 // linear probing
		if g >= groupCount {
			g = 0
		}
	}
}

// Delete attempts to remove |key|, returns true successful.
func (m *Map[K, V]) Delete(key K) (ok bool) {
	var (
		groupCount = uint32(len(m.groups))
		hi, lo     = splitHash(Hash(m.hash.Hash(key)))
		g          = probeStart(hi, groupCount)
		i          uint32
	)
	for {
		var (
			ctrl    = &m.ctrl[g]
			group   = &m.groups[g]
			matches = metaMatchH2(ctrl, lo)
		)
		for matches != 0 {
			i, matches = nextMatch(matches)
			if key == group.keys[i] {
				ok = true
				// optimization: if |m.ctrl[g]| contains any empty
				// metadata bytes, we can physically delete |key|
				// rather than placing a tombstone.
				// The observation is that any probes into mapGroup |g|
				// would already be terminated by the existing empty
				// slot, and therefore reclaiming slot |s| will not
				// cause premature termination of probes into |g|.
				if metaMatchEmpty(ctrl) != 0 {
					ctrl[i] = empty
					m.resident--
				} else {
					ctrl[i] = tombstone
					m.dead++
				}
				var k K
				var v V
				group.keys[i] = k
				group.values[i] = v
				return
			}
		}
		// |key| is not in mapGroup |g|,
		// stop probing if we see an empty slot
		matches = metaMatchEmpty(ctrl)
		if matches != 0 { // |key| absent
			ok = false
			return
		}
		g += 1 // linear probing
		if g >= groupCount {
			g = 0
		}
	}
}

// Iter iterates the elements of the Map, passing them to the callback.
// It guarantees that any key in the Map will be visited only once, and
// for un-mutated Maps, every key will be visited once. If the Map is
// Mutated during iteration, mutations will be reflected on return from
// Iter, but the set of keys visited by Iter is non-deterministic.
func (m *Map[K, V]) Iter(cb func(k K, v V) (stop bool)) {
	// take a consistent view of the table in case
	// we rehash during iteration
	var (
		ctrl, groups = m.ctrl, m.groups
		groupCount   = uint32(len(groups))
		g            = randIntN(groupCount) // pick a random starting mapGroup
	)
	for n := 0; n < len(groups); n++ {
		group := &groups[g]
		for i, c := range ctrl[g] {
			if c == empty || c == tombstone {
				continue
			}
			k, v := group.keys[i], group.values[i]
			if stop := cb(k, v); stop {
				return
			}
		}
		g++
		if g >= groupCount {
			g = 0
		}
	}
}

// Clear removes all elements from the Map.
func (m *Map[K, V]) Clear() {
	for i := range m.ctrl {
		m.ctrl[i] = emptyMeta
	}
	emptyGroup := mapGroup[K, V]{}
	for i := range m.groups {
		m.groups[i] = emptyGroup
	}
	m.resident, m.dead = 0, 0
}

// Count returns the number of elements in the Map.
func (m *Map[K, V]) Count() int {
	return int(m.resident - m.dead)
}

// Capacity returns the total number of items that can
// the can be added to the Map before resizing.
func (m *Map[K, V]) Capacity() int {
	return int(m.limit)
}

// UnusedCapacity returns the number of additional elements
// the can be added to the Map before resizing.
func (m *Map[K, V]) UnusedCapacity() int {
	return int(m.limit - m.resident)
}

// find returns the location of |key| if present, or its insertion location if absent.
// for performance, find is manually inlined into public methods.
func (m *Map[K, V]) find(key K, hi h1, lo h2) (g, i uint32, ok bool) {
	groupCount := uint32(len(m.groups))
	g = probeStart(hi, groupCount)
	for {
		ctrl := &m.ctrl[g]
		group := &m.groups[g]
		matches := metaMatchH2(ctrl, lo)
		for matches != 0 {
			i, matches = nextMatch(matches)
			if key == group.keys[i] {
				return g, i, true
			}
		}
		// |key| is not in mapGroup |g|,
		// stop probing if we see an empty slot
		matches = metaMatchEmpty(ctrl)
		if matches != 0 {
			i, _ = nextMatch(matches)
			return g, i, false
		}
		g += 1 // linear probing
		if g >= groupCount {
			g = 0
		}
	}
}

func (m *Map[K, V]) nextSize() (n uint32) {
	n = uint32(len(m.groups)) * 2
	if m.dead >= (m.resident / 2) {
		n = uint32(len(m.groups))
	}
	return
}

func (m *Map[K, V]) rehash(n uint32) {
	oldGroups, oldCtrl := m.groups, m.ctrl
	m.hash = maphash.NewSeed(m.hash)
	m.limit = n * maxAvgGroupLoad
	m.resident, m.dead = 0, 0
	m.groups = make([]mapGroup[K, V], n)
	m.ctrl = make([]groupMetadata, n)
	for i := range m.ctrl {
		m.ctrl[i] = emptyMeta
	}
	for c, oldCtrl := range oldCtrl {
		oldGroup := &oldGroups[c]
		for i, c := range oldCtrl {
			if c == empty || c == tombstone {
				continue
			}
			m.Put(oldGroup.keys[i], oldGroup.values[i])
		}
	}
}

func (m *Map[K, V]) loadFactor() float32 {
	slots := float32(len(m.groups) * groupSize)
	return float32(m.resident-m.dead) / slots
}
