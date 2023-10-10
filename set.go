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

package swiss

// Set is a derivative work from the Map container created by Dolthub, Inc
// and available at https://github.com/dolthub/swiss/.  Dolthub's Map is an
// open-addressing hash map based on Abseil's flat_hash_map.
//
// Set incorporates significant to work as a set rather than as a map, and requires callers
// to provide thier own hash.  It also incorporates several small performance optimisations.
type Set[V comparable] struct {
	ctrl     []metadata
	groups   []setGroup[V]
	resident uint32
	dead     uint32
	limit    uint32
}

// SetPair is a hash and value pair.
type SetPair[V comparable] struct {
	Hash  Hash
	Value V
}

// setGroup is a group of 16 hash-value pairs.
type setGroup[V comparable] struct {
	hashes [groupSize]Hash
	values [groupSize]V
}

// NewSet constructs a Set.
func NewSet[V comparable](sz uint32) (s *Set[V]) {
	n := numGroups(sz)
	s = &Set[V]{
		ctrl:   make([]metadata, n),
		groups: make([]setGroup[V], n),
		limit:  n * maxAvgGroupLoad,
	}
	for i := range s.ctrl {
		s.ctrl[i] = emptyMeta
	}
	return
}

// Has returns true if |hash| is present in |s|.
func (s *Set[V]) HasHash(hash Hash) (ok bool) {
	hi, lo := splitHash(hash)
	// g is the index of the group to start, identified by the remainder
	// from dividing the hash by the number of groups.  Starting with this
	// group, the the lower bits from the hash (H2) are checked for existence
	// within the group using a 16-way SSE instruction.  If there is a match,
	// each matching entry in the group is then checked to see if the remainder
	// of the hash matches what we are looking for.
	g := probeStart(hi, len(s.groups))
	var i uint32
	lastGroupIndex := uint32(len(s.groups))
	for { // inlined find loop
		ctrl := &s.ctrl[g]
		matches := metaMatchH2(ctrl, lo)
		for matches != 0 {
			group := &s.groups[g]
			i, matches = nextMatch(matches)
			if hash == group.hashes[i] {
				ok = true
				return
			}
		}
		// |hash| is not in group |g|,
		// stop probing if we see an empty slot
		matches = metaMatchEmpty(ctrl)
		if matches != 0 {
			ok = false
			return
		}
		g += 1 // linear probing
		if g >= lastGroupIndex {
			g = 0
		}
	}
}

// Has returns true if |hash| and |value| is present in |s|.
func (s *Set[V]) Has(hash Hash, value V) (ok bool) {
	hi, lo := splitHash(hash)
	g := probeStart(hi, len(s.groups))
	var i uint32
	lastGroupIndex := uint32(len(s.groups))
	for { // inlined find loop
		ctrl := &s.ctrl[g]
		matches := metaMatchH2(ctrl, lo)
		for matches != 0 {
			group := &s.groups[g]
			i, matches = nextMatch(matches)
			if hash == group.hashes[i] && value == group.values[i] {
				ok = true
				return
			}
		}
		// |hash|value| is not in group |g|,
		// stop probing if we see an empty slot
		matches = metaMatchEmpty(ctrl)
		if matches != 0 {
			ok = false
			return
		}
		g += 1 // linear probing
		if g >= lastGroupIndex {
			g = 0
		}
	}
}

// Get returns the |value|s mapped by |hash|.
func (s *Set[V]) Get(hash Hash, valueStorage []V) (values []V, valueCount uint) {
	values = valueStorage
	hi, lo := splitHash(hash)
	g := probeStart(hi, len(s.groups))
	var i uint32
	lastGroupIndex := uint32(len(s.groups))
	for { // inlined find loop
		ctrl := &s.ctrl[g]
		matches := metaMatchH2(ctrl, lo)
		for matches != 0 {
			group := &s.groups[g]
			i, matches = nextMatch(matches)
			if hash == group.hashes[i] {
				values = append(values, group.values[i])
				valueCount++
			}
		}
		// |hash| may or may not be in group |g|,
		// stop probing if we see an empty slot
		matches = metaMatchEmpty(ctrl)
		if matches != 0 {
			return
		}
		g += 1 // linear probing
		if g >= lastGroupIndex {
			g = 0
		}
	}
}

// GetHashes returns the set of all hashes.  There may be duplicates.
func (s *Set[V]) GetHashes() []Hash {
	hashes := make([]Hash, s.Count())
	hashIndex := uint(0)

	// pick a random starting group
	g := randIntN(len(s.groups))
	lastGroupIndex := uint32(len(s.groups))
	for n := 0; n < len(s.groups); n++ {
		group := &s.groups[g]
		for i, c := range s.ctrl[g] {
			if c == tombstone {
				continue
			}
			if c == empty {
				// No more hashes in this group
				break
			}
			hashes[hashIndex] = group.hashes[i]
			hashIndex++
		}
		g++
		if g >= lastGroupIndex {
			g = 0
		}
	}

	return hashes
}

// GetValues returns the set of all values.  There may be duplicates.
func (s *Set[V]) GetValues() []V {
	values := make([]V, s.Count())
	valueIndex := uint(0)

	// pick a random starting group
	g := randIntN(len(s.groups))
	lastGroupIndex := uint32(len(s.groups))
	for n := 0; n < len(s.groups); n++ {
		group := &s.groups[g]
		for i, c := range s.ctrl[g] {
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
		if g >= lastGroupIndex {
			g = 0
		}
	}

	return values
}

// GetPairs returns the set of all hash and value pairs.  Each pair is unique.
func (s *Set[V]) GetPairs() []SetPair[V] {
	pairs := make([]SetPair[V], s.Count())
	pairIndex := uint(0)

	// pick a random starting group
	g := randIntN(len(s.groups))
	lastGroupIndex := uint32(len(s.groups))
	for n := 0; n < len(s.groups); n++ {
		group := &s.groups[g]
		for i, c := range s.ctrl[g] {
			if c == tombstone {
				continue
			}
			if c == empty {
				// No more entries in this group
				break
			}
			pairs[pairIndex] = SetPair[V]{group.hashes[i], group.values[i]}
			pairIndex++
		}
		g++
		if g >= lastGroupIndex {
			g = 0
		}
	}

	return pairs
}

// Put attempts to insert |hash| and |value|
func (s *Set[V]) Put(hash Hash, value V) {
	if s.resident >= s.limit {
		s.resize(s.nextSize())
	}
	hi, lo := splitHash(hash)
	g := probeStart(hi, len(s.groups))
	var i uint32
	lastGroupIndex := uint32(len(s.groups))
	for { // inlined find loop
		ctrl := &s.ctrl[g]
		group := &s.groups[g]
		matches := metaMatchH2(ctrl, lo)
		for matches != 0 {
			i, matches = nextMatch(matches)
			if hash == group.hashes[i] && value == group.values[i] {
				// Hash / value pair exists
				return
			}
		}
		// |hash|value| is not in group |g|, stop probing if we see an empty slot
		matches = metaMatchEmpty(ctrl)
		if matches != 0 { // insert
			i, _ = nextMatch(matches)
			group.hashes[i] = hash
			group.values[i] = value
			ctrl[i] = int8(lo)
			s.resident++
			return
		}
		g += 1 // linear probing
		if g >= lastGroupIndex {
			g = 0
		}
	}
}

// Delete attempts to remove |hash| and |value|, returns true successful.
func (s *Set[V]) Delete(hash Hash, value V) (ok bool) {
	hi, lo := splitHash(hash)
	g := probeStart(hi, len(s.groups))
	var i uint32
	lastGroupIndex := uint32(len(s.groups))
	for {
		ctrl := &s.ctrl[g]
		group := &s.groups[g]
		matches := metaMatchH2(ctrl, lo)
		for matches != 0 {
			i, matches = nextMatch(matches)
			if hash == group.hashes[i] && value == group.values[i] {
				ok = true
				// optimization: if |s.ctrl[g]| contains any empty
				// metadata bytes, we can physically delete |key|
				// rather than placing a tombstone.
				// The observation is that any probes into group |g|
				// would already be terminated by the existing empty
				// slot, and therefore reclaiming slot |s| will not
				// cause premature termination of probes into |g|.
				if metaMatchEmpty(ctrl) != 0 {
					ctrl[i] = empty
					s.resident--
				} else {
					ctrl[i] = tombstone
					s.dead++
				}
				var h Hash
				var v V
				group.hashes[i] = h
				group.values[i] = v
				return
			}
		}
		// |hash|value| is not in group |g|,
		// stop probing if we see an empty slot
		matches = metaMatchEmpty(ctrl)
		if matches != 0 { // |hash|value| absent
			ok = false
			return
		}
		g += 1 // linear probing
		if g >= lastGroupIndex {
			g = 0
		}
	}
}

// Iter iterates the elements of the Map, passing them to the callback.
// It guarantees that any key in the Map will be visited only once, and
// for un-mutated Maps, every key will be visited once. If the Map is
// Mutated during iteration, mutations will be reflected on return from
// Iter, but the set of keys visited by Iter is non-deterministic.
func (s *Set[V]) Iter(cb func(h Hash, v V) (stop bool)) {
	// take a consistent view of the table in case we rehash during iteration
	ctrl, groups := s.ctrl, s.groups
	// pick a random starting group
	g := randIntN(len(groups))
	lastGroupIndex := uint32(len(s.groups))
	for n := 0; n < len(groups); n++ {
		group := &s.groups[g]
		for i, c := range ctrl[g] {
			if c == empty || c == tombstone {
				continue
			}
			h, v := group.hashes[i], group.values[i]
			if stop := cb(h, v); stop {
				return
			}
		}
		g++
		if g >= lastGroupIndex {
			g = 0
		}
	}
}

// Destruct resets the set to a zero'd state
func (s *Set[V]) Destruct() {
	s.ctrl = nil
	s.groups = nil
	s.resident = 0
	s.dead = 0
	s.limit = 0
}

// Clear removes all elements from the Map.
func (s *Set[V]) Clear() {
	for i := range s.ctrl {
		s.ctrl[i] = emptyMeta
	}
	emptyGroup := setGroup[V]{}
	for i := range s.groups {
		s.groups[i] = emptyGroup
	}
	s.resident, s.dead = 0, 0
}

// Count returns the number of elements in the Map.
func (s *Set[V]) Count() uint {
	return uint(s.resident - s.dead)
}

// Capacity returns the total number of items that can
// the can be added to the Map before resizing.
func (s *Set[V]) Capacity() uint {
	return uint(s.limit)
}

// UnusedCapacity returns the number of additional elements
// the can be added to the Map before resizing.
func (s *Set[V]) UnusedCapacity() uint {
	return uint(s.limit - s.resident)
}

// find returns the location of |hash| if present, or its insertion location if absent.
// for performance, find is manually inlined into public methods.
func (s *Set[V]) find(hash Hash, value V) (g, i uint32, ok bool) {
	hi, lo := splitHash(hash)
	g = probeStart(hi, len(s.groups))
	lastGroupIndex := uint32(len(s.groups))
	for {
		ctrl := &s.ctrl[g]
		group := &s.groups[g]
		matches := metaMatchH2(ctrl, lo)
		for matches != 0 {
			i, matches = nextMatch(matches)
			if hash == group.hashes[i] && value == group.values[i] {
				return g, i, true
			}
		}
		// |hash|value| is not in group |g|,
		// stop probing if we see an empty slot
		matches = metaMatchEmpty(ctrl)
		if matches != 0 {
			i, _ = nextMatch(matches)
			return g, i, false
		}
		g += 1 // linear probing
		if g >= lastGroupIndex {
			g = 0
		}
	}
}

func (s *Set[V]) nextSize() (n uint32) {
	n = uint32(len(s.groups)) * 2
	if s.dead >= (s.resident / 2) {
		n = uint32(len(s.groups))
	}
	return
}

func (s *Set[V]) resize(n uint32) {
	oldGroups, oldCtrl := s.groups, s.ctrl
	s.limit = n * maxAvgGroupLoad
	s.resident, s.dead = 0, 0
	s.ctrl = make([]metadata, n)
	s.groups = make([]setGroup[V], n)
	for i := range s.ctrl {
		s.ctrl[i] = emptyMeta
	}
	// It is considered best practice to rehash / change seed during a resize, however
	// we need to retain what we have been given as this container is not responsible
	// for creating the hashes.
	for c, oldCtrl := range oldCtrl {
		oldGroup := &oldGroups[c]
		for i, c := range oldCtrl {
			if c == empty || c == tombstone {
				continue
			}
			s.Put(oldGroup.hashes[i], oldGroup.values[i])
		}
	}
}

func (s *Set[V]) loadFactor() float32 {
	slots := float32(len(s.groups) * groupSize)
	return float32(s.resident-s.dead) / slots
}
