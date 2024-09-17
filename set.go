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
	meta       []int8
	hashes     []Hash
	values     []V
	groupCount uint32
	resident   uint32
	dead       uint32
	limit      uint32
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
	var (
		groupCount = numGroups(sz)
		capacity   = (groupCount << groupBits) // groupCount * groupSize
	)
	s = &Set[V]{
		meta:       make([]int8, capacity),
		hashes:     make([]Hash, capacity),
		values:     make([]V, capacity),
		groupCount: groupCount,
		limit:      groupCount * maxAvgGroupLoad,
	}
	for i := range s.meta {
		s.meta[i] = empty
	}
	return
}

// Has returns true if |hash| is present in |s|.
func (s *Set[V]) HasHash(hash Hash) (ok bool) {
	// g is the index of the group to start, identified by the remainder
	// from dividing the hash by the number of groups.  Starting with this
	// group, the the lower bits from the hash (H2) are checked for existence
	// within the group using a 16-way SSE instruction.  If there is a match,
	// each matching entry in the group is then checked to see if the remainder
	// of the hash matches what we are looking for.
	var (
		groupCount = s.groupCount
		hi, lo     = splitHash(hash)
		g          = probeStart(hi, groupCount)
		i          uint32
	)
	for { // inlined find loop
		var (
			groupMeta = (*groupMetadata)(s.meta[g<<groupBits:])
			matches   = metaMatchH2(groupMeta, lo)
		)
		for matches != 0 {
			hashes := (*groupHashes)(s.hashes[g<<groupBits:])
			i, matches = nextMatch(matches)
			if hash == hashes[i] {
				ok = true
				return
			}
		}
		// |hash| is not in group |g|,
		// stop probing if we see an empty slot
		matches = metaMatchEmpty(groupMeta)
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

// Has returns true if |hash| and |value| is present in |s|.
func (s *Set[V]) Has(hash Hash, value V) (ok bool) {
	var (
		groupCount = s.groupCount
		hi, lo     = splitHash(hash)
		g          = probeStart(hi, groupCount)
		i          uint32
	)
	for { // inlined find loop
		var (
			groupOffset = g << groupBits
			groupMeta   = (*groupMetadata)(s.meta[groupOffset:])
			groupHashes = (*groupHashes)(s.hashes[groupOffset:])
			matches     = metaMatchH2(groupMeta, lo)
		)
		for matches != 0 {
			i, matches = nextMatch(matches)
			if hash == groupHashes[i] && value == s.values[groupOffset+i] {
				ok = true
				return
			}
		}
		// |hash|value| is not in group |g|,
		// stop probing if we see an empty slot
		matches = metaMatchEmpty(groupMeta)
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

// Get returns the |value|s mapped by |hash|.
func (s *Set[V]) Get(hash Hash, valueStorage []V) (values []V, valueCount uint) {
	values = valueStorage
	var (
		groupCount = s.groupCount
		hi, lo     = splitHash(hash)
		g          = probeStart(hi, groupCount)
		i          uint32
	)
	for { // inlined find loop
		var (
			groupStart  = g << groupBits
			groupMeta   = (*groupMetadata)(s.meta[groupStart:])
			groupHashes = (*groupHashes)(s.hashes[groupStart:])
			matches     = metaMatchH2(groupMeta, lo)
		)
		for matches != 0 {
			i, matches = nextMatch(matches)
			if hash == groupHashes[i] {
				values = append(values, s.values[groupStart+i])
				valueCount++
			}
		}
		// |hash| may or may not be in group |g|,
		// stop probing if we see an empty slot
		matches = metaMatchEmpty(groupMeta)
		if matches != 0 {
			return
		}
		g += 1 // linear probing
		if g >= groupCount {
			g = 0
		}
	}
}

type HashIterator func(hash Hash)

// IterateHashes returns the set of all hashes.  There may be duplicates.
func (s *Set[V]) IterateHashes(callback HashIterator) {
	var (
		groupCount = s.groupCount
		g          = randIntN(groupCount) // pick a random starting group
	)
	for n := uint32(0); n < groupCount; n++ {
		var (
			groupOffset = g << groupBits
			groupMeta   = (*groupMetadata)(s.meta[groupOffset:])
			groupHashes = (*groupHashes)(s.hashes[groupOffset:])
		)
		for i, c := range groupMeta {
			if c == tombstone {
				continue
			}
			if c == empty {
				// No more hashes in this group
				break
			}
			callback(groupHashes[i])
		}
		g++
		if g >= groupCount {
			g = 0
		}
	}
}

// GetHashes returns the set of all hashes.  There may be duplicates.
func (s *Set[V]) GetHashes() []Hash {
	var (
		groupCount = s.groupCount
		hashes     = make([]Hash, s.Count())
		g          = randIntN(groupCount) // pick a random starting group
		hashIndex  = uint(0)
	)
	for n := uint32(0); n < groupCount; n++ {
		var (
			groupOffset = g << groupBits
			groupMeta   = (*groupMetadata)(s.meta[groupOffset:])
			groupHashes = (*groupHashes)(s.hashes[groupOffset:])
		)
		for i, c := range groupMeta {
			if c == tombstone {
				continue
			}
			if c == empty {
				// No more hashes in this group
				break
			}
			hashes[hashIndex] = groupHashes[i]
			hashIndex++
		}
		g++
		if g >= groupCount {
			g = 0
		}
	}

	return hashes
}

// GetValues returns the set of all values.  There may be duplicates.
func (s *Set[V]) GetValues() []V {
	var (
		groupCount = s.groupCount
		values     = make([]V, s.Count())
		g          = randIntN(groupCount) // pick a random starting group
		valueIndex = uint(0)
	)
	for n := uint32(0); n < groupCount; n++ {
		var (
			groupOffset = int(g) << groupBits
			groupMeta   = (*groupMetadata)(s.meta[groupOffset:])
		)
		for i, c := range groupMeta {
			if c == tombstone {
				continue
			}
			if c == empty {
				// No more hashes in this group
				break
			}
			values[valueIndex] = s.values[groupOffset+i]
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
func (s *Set[V]) GetPairs() []SetPair[V] {
	var (
		groupCount = s.groupCount
		pairs      = make([]SetPair[V], s.Count())
		pairIndex  = uint(0)
		g          = randIntN(groupCount) // pick a random starting group
	)
	for n := uint32(0); n < groupCount; n++ {
		var (
			groupOffset = int(g) << groupBits
			groupMeta   = (*groupMetadata)(s.meta[groupOffset:])
			groupHashes = (*groupHashes)(s.hashes[groupOffset:])
		)
		for i, c := range groupMeta {
			if c == tombstone {
				continue
			}
			if c == empty {
				// No more entries in this group
				break
			}
			pairs[pairIndex] = SetPair[V]{groupHashes[i], s.values[groupOffset+i]}
			pairIndex++
		}
		g++
		if g >= groupCount {
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
	var (
		groupCount = s.groupCount
		hi, lo     = splitHash(hash)
		g          = probeStart(hi, groupCount)
		i          uint32
	)
	for { // inlined find loop
		var (
			groupOffset = g << groupBits
			groupMeta   = (*groupMetadata)(s.meta[groupOffset:])
			groupHashes = (*groupHashes)(s.hashes[groupOffset:])
			matches     = metaMatchH2(groupMeta, lo)
		)
		for matches != 0 {
			i, matches = nextMatch(matches)
			if hash == groupHashes[i] && value == s.values[groupOffset+i] {
				// Hash / value pair exists
				return
			}
		}
		// |hash|value| is not in group |g|, stop probing if we see an empty slot
		matches = metaMatchEmpty(groupMeta)
		if matches != 0 { // insert
			i, _ = nextMatch(matches)
			groupHashes[i] = hash
			s.values[groupOffset+i] = value
			groupMeta[i] = int8(lo)
			s.resident++
			return
		}
		g += 1 // linear probing
		if g >= groupCount {
			g = 0
		}
	}
}

// Delete attempts to remove |hash| and |value|, returns true successful.
func (s *Set[V]) Delete(hash Hash, value V) (ok bool) {
	var (
		groupCount = s.groupCount
		hi, lo     = splitHash(hash)
		g          = probeStart(hi, groupCount)
		i          uint32
	)
	for {
		var (
			groupOffset = g << groupBits
			groupMeta   = (*groupMetadata)(s.meta[groupOffset:])
			groupHashes = (*groupHashes)(s.hashes[groupOffset:])
			matches     = metaMatchH2(groupMeta, lo)
		)
		for matches != 0 {
			i, matches = nextMatch(matches)
			if hash == groupHashes[i] && value == s.values[groupOffset+i] {
				ok = true
				// optimization: if |s.ctrl[g]| contains any empty
				// metadata bytes, we can physically delete |key|
				// rather than placing a tombstone.
				// The observation is that any probes into group |g|
				// would already be terminated by the existing empty
				// slot, and therefore reclaiming slot |s| will not
				// cause premature termination of probes into |g|.
				if metaMatchEmpty(groupMeta) != 0 {
					groupMeta[i] = empty
					s.resident--
				} else {
					groupMeta[i] = tombstone
					s.dead++
				}
				var h Hash
				var v V
				groupHashes[i] = h
				s.values[groupOffset+i] = v
				return
			}
		}
		// |hash|value| is not in group |g|,
		// stop probing if we see an empty slot
		matches = metaMatchEmpty(groupMeta)
		if matches != 0 { // |hash|value| absent
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
func (s *Set[V]) Iter(cb func(h Hash, v V) (stop bool)) {
	// take a consistent view of the table in case we rehash during iteration
	var (
		meta, hashes = s.meta, s.hashes
		groupCount   = s.groupCount
		g            = randIntN(groupCount) // pick a random starting group
	)
	for n := uint32(0); n < groupCount; n++ {
		var (
			groupOffset = int(g) << groupBits
			groupMeta   = (*groupMetadata)(meta[groupOffset:])
			groupHashes = (*groupHashes)(hashes[groupOffset:])
		)
		for i, c := range groupMeta {
			if c == empty || c == tombstone {
				continue
			}
			if stop := cb(groupHashes[i], s.values[groupOffset+i]); stop {
				return
			}
		}
		g++
		if g >= groupCount {
			g = 0
		}
	}
}

// Destruct resets the set to a zero'd state
func (s *Set[V]) Destruct() {
	s.meta = nil
	s.hashes = nil
	s.values = nil
	s.groupCount = 0
	s.resident = 0
	s.dead = 0
	s.limit = 0
}

// Clear removes all elements from the Map.
func (s *Set[V]) Clear() {
	for i := range s.meta {
		s.meta[i] = empty
	}
	for i := range s.hashes {
		s.hashes[i] = 0
	}
	var emptyValue V
	for i := range s.values {
		s.values[i] = emptyValue
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
	var (
		meta, hashes = s.meta, s.hashes
		groupCount   = s.groupCount
		hi, lo       = splitHash(hash)
	)
	g = probeStart(hi, groupCount)
	for {
		var (
			groupOffset = g << groupBits
			groupMeta   = (*groupMetadata)(meta[groupOffset:])
			groupHashes = (*groupHashes)(hashes[groupOffset:])
			matches     = metaMatchH2(groupMeta, lo)
		)
		for matches != 0 {
			i, matches = nextMatch(matches)
			if hash == groupHashes[i] && value == s.values[groupOffset+i] {
				return g, i, true
			}
		}
		// |hash|value| is not in group |g|,
		// stop probing if we see an empty slot
		matches = metaMatchEmpty(groupMeta)
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

func (s *Set[V]) nextSize() (n uint32) {
	n = s.groupCount
	if s.dead <= (s.resident / 2) {
		n += n // double
	}
	return
}

func (s *Set[V]) resize(groupCount uint32) {
	var (
		oldMeta, oldHashes, oldValues, oldGroupCount = s.meta, s.hashes, s.values, s.groupCount
		capacity                                     = (groupCount << groupBits) // groupCount * groupSize
	)
	s.limit = groupCount * maxAvgGroupLoad
	s.resident, s.dead = 0, 0
	s.groupCount = groupCount
	s.meta = make([]int8, capacity)
	s.hashes = make([]Hash, capacity)
	s.values = make([]V, capacity)
	for i := range s.meta {
		s.meta[i] = empty
	}
	// It is considered best practice to rehash / change seed during a resize, however
	// we need to retain what we have been given as this container is not responsible
	// for creating the hashes.
	for g := uint32(0); g < oldGroupCount; g++ {
		var (
			oldGroupOffset = int(g) << groupBits
			oldGroupMeta   = (*groupMetadata)(oldMeta[oldGroupOffset:])
			oldGroupHashes = (*groupHashes)(oldHashes[oldGroupOffset:])
		)
		for i, c := range oldGroupMeta {
			if c == empty || c == tombstone {
				continue
			}
			s.Put(oldGroupHashes[i], oldValues[oldGroupOffset+i])
		}
	}
}

func (s *Set[V]) loadFactor() float32 {
	slots := float32(s.groupCount) * groupSize
	return float32(s.resident-s.dead) / slots
}
