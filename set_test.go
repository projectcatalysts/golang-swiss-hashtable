// Copyright 2023 Dolthub, Inc.
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

import (
	"encoding/binary"
	"hash/fnv"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

func hash64_String(s string) Hash {
	hasher := fnv.New64a()
	hasher.Write([]byte(s))
	return Hash(hasher.Sum64())
}

func hash64_UInt32(s uint32) Hash {
	hasher := fnv.New64a()
	bytes := [4]byte{}
	binary.LittleEndian.PutUint32(bytes[:], s)
	hasher.Write(bytes[:])
	return Hash(hasher.Sum64())
}

func hash64_Int64(s int64) Hash {
	hasher := fnv.New64a()
	bytes := [8]byte{}
	binary.LittleEndian.PutUint64(bytes[:], uint64(s))
	hasher.Write(bytes[:])
	return Hash(hasher.Sum64())
}

type hashAndValue[V comparable] struct {
	hash  Hash
	value V
}

func TestSwissSet(t *testing.T) {
	t.Run("strings=0", func(t *testing.T) {
		testSwissSet(t, genStringSetData(16, 0, hash64_String))
	})
	t.Run("strings=100", func(t *testing.T) {
		testSwissSet(t, genStringSetData(16, 100, hash64_String))
	})
	t.Run("strings=1000", func(t *testing.T) {
		testSwissSet(t, genStringSetData(16, 1000, hash64_String))
	})
	t.Run("strings=10_000", func(t *testing.T) {
		testSwissSet(t, genStringSetData(16, 10_000, hash64_String))
	})
	t.Run("strings=100_000", func(t *testing.T) {
		testSwissSet(t, genStringSetData(16, 100_000, hash64_String))
	})
	t.Run("uint32=0", func(t *testing.T) {
		testSwissSet(t, genUint32SetData(0, hash64_UInt32))
	})
	t.Run("uint32=100", func(t *testing.T) {
		testSwissSet(t, genUint32SetData(100, hash64_UInt32))
	})
	t.Run("uint32=1000", func(t *testing.T) {
		testSwissSet(t, genUint32SetData(1000, hash64_UInt32))
	})
	t.Run("uint32=10_000", func(t *testing.T) {
		testSwissSet(t, genUint32SetData(10_000, hash64_UInt32))
	})
	t.Run("uint32=100_000", func(t *testing.T) {
		testSwissSet(t, genUint32SetData(100_000, hash64_UInt32))
	})
	t.Run("string capacity", func(t *testing.T) {
		testSwissSetCapacity[string](t, func(n int) []hashAndValue[string] {
			return genStringSetData(16, n, hash64_String)
		})
	})
	t.Run("uint32 capacity", func(t *testing.T) {
		testSwissSetCapacity[uint32](t, func(n int) []hashAndValue[uint32] {
			return genUint32SetData(n, hash64_UInt32)
		})
	})
}

// testSwissSet runs a suite of tests against a data set
func testSwissSet[V comparable](t *testing.T, values []hashAndValue[V]) {
	// validate that the length of the values matches the length of unique values.
	// Hashes are not used in the uniqueness check.
	require.Equal(t, uint(len(values)), uint(len(uniq(values))), values)
	t.Run("put", func(t *testing.T) {
		testSetPut(t, values)
	})
	t.Run("has", func(t *testing.T) {
		testSetHas(t, values)
	})
	t.Run("get", func(t *testing.T) {
		testSetGet(t, values)
	})
	t.Run("delete", func(t *testing.T) {
		testSetDelete(t, values)
	})
	t.Run("clear", func(t *testing.T) {
		testSetClear(t, values)
	})
	t.Run("iter", func(t *testing.T) {
		testSetIter(t, values)
	})
	t.Run("grow", func(t *testing.T) {
		testSetGrow(t, values)
	})
	t.Run("probe stats", func(t *testing.T) {
		testSetProbeStats(t, values)
	})
}

// genStringSetData returns a slice of string values and their associated hashed value
// - size represents the target rune length of each value
// - count represents the number of strings to return
func genStringSetData(size, count int, hasher func(string) Hash) (values []hashAndValue[string]) {
	src := rand.New(rand.NewSource(int64(size * count)))
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	// r is a slice of runes that contains the contents of all generated strings before they are sliced up
	// into individal strings of length size.
	r := make([]rune, size*count)
	for i := range r {
		r[i] = letters[src.Intn(len(letters))]
	}
	values = make([]hashAndValue[string], count)
	for i := range values {
		values[i].value = string(r[:size])
		values[i].hash = hasher(values[i].value)
		r = r[size:]
	}
	return
}

// genUint32SetData returns a slice of uint32 values and their associated hashed value
// - count represents the count of uint32 values to return
func genUint32SetData(count int, hasher func(uint32) Hash) (values []hashAndValue[uint32]) {
	values = make([]hashAndValue[uint32], count)
	var x uint32
	for i := range values {
		x += (rand.Uint32() % 128) + 1
		values[i].value = x
		values[i].hash = hasher(values[i].value)
	}
	return
}

// generateInt64SetData returns a slice of int64 values and their associated hashed value
// - count represents the count of int64 values to return
func generateInt64SetData(n int, hasher func(int64) Hash) (data []hashAndValue[int64]) {
	data = make([]hashAndValue[int64], n)
	var x int64
	for i := range data {
		x += rand.Int63n(128) + 1
		data[i].value = x
		data[i].hash = hasher(data[i].value)
	}
	return
}

// testSetPut creates a new map and adds all of the values to the map.
// It then runs through and adds them again and verifies that the count
// of values remains constant after the second addition.  It then iterates
// through the vales and retrieves them from the map and verifies that
// the value can be retrieved.
func testSetPut[V comparable](t *testing.T, values []hashAndValue[V]) {
	s := NewSet[V](uint32(len(values)))
	assert.Equal(t, uint(0), s.Count())
	for _, v := range values {
		s.Put(v.hash, v.value)
	}
	assert.Equal(t, uint(len(values)), s.Count())
	// put the values again.  This should not change the count because
	// all values should be already there.
	for _, v := range values {
		s.Put(v.hash, v.value)
	}
	assert.Equal(t, uint(len(values)), s.Count())
	// check that the values can be retrieved
	foundStorage := [1]V{}
	for _, v := range values {
		found := foundStorage[:0]
		foundCount := s.Get(v.hash, &found)
		assert.Equal(t, foundCount, uint(1))
		assert.Equal(t, found[0], v.value)
	}
	assert.Equal(t, uint(len(values)), uint(s.resident))
}

// testSetHas adds all of the values to the map and checks them for existence
// based on the hashed value only.
func testSetHas[V comparable](t *testing.T, values []hashAndValue[V]) {
	s := NewSet[V](uint32(len(values)))
	for _, v := range values {
		s.Put(v.hash, v.value)
	}
	for _, v := range values {
		ok := s.HasHash(v.hash)
		assert.True(t, ok)
		ok = s.Has(v.hash, v.value)
		assert.True(t, ok)
	}
	// try to find a non-existent value
	if len(values) > 0 {
		hasHash := s.HasHash(1234)
		assert.False(t, hasHash)
		has := s.Has(1234, values[0].value)
		assert.False(t, has)
	}
}

// testSetGet adds all of the values to the map and checks them for existence
// based on the hash and value.
func testSetGet[V comparable](t *testing.T, values []hashAndValue[V]) {
	s := NewSet[V](uint32(len(values)))
	for _, v := range values {
		s.Put(v.hash, v.value)
	}
	foundStorage := [1]V{}
	for _, v := range values {
		found := foundStorage[:0]
		foundCount := s.Get(v.hash, &found)
		assert.Equal(t, foundCount, uint(1))
		assert.Equal(t, found[0], v.value)
	}
}

// testSetDelete adds all of the values to the map and then deletes them all,
// and calls Has to verify that the hash and value no longer exist.
func testSetDelete[V comparable](t *testing.T, values []hashAndValue[V]) {
	s := NewSet[V](uint32(len(values)))
	assert.Equal(t, uint(0), s.Count())
	for _, v := range values {
		s.Put(v.hash, v.value)
	}
	assert.Equal(t, uint(len(values)), s.Count())
	foundStorage := [1]V{}
	for _, v := range values {
		found := foundStorage[:0]
		foundCount1 := s.Get(v.hash, &found)
		isDeleted := s.Delete(v.hash, v.value)
		assert.True(t, isDeleted)
		foundCount2 := s.Get(v.hash, &found)
		assert.Equal(t, foundCount1-1, foundCount2)
	}
	assert.Equal(t, uint(0), s.Count())
	// put keys back after deleting them
	for _, v := range values {
		s.Put(v.hash, v.value)
	}
	assert.Equal(t, uint(len(values)), s.Count())
	// try to delete a non-existent value
	if len(values) > 0 {
		isDeleted := s.Delete(1234, values[0].value)
		assert.False(t, isDeleted)
	}
}

// testSetClear adds the set of values to the map, clears the map,
// then uses the Has and Get functions to validate that the values
// are not present in the map.  It then iterates through the map and
// counts the values to ensure the result is zero.
func testSetClear[V comparable](t *testing.T, values []hashAndValue[V]) {
	s := NewSet[V](0)
	assert.Equal(t, uint(0), s.Count())
	for _, v := range values {
		s.Put(v.hash, v.value)
	}
	assert.Equal(t, uint(len(values)), s.Count())
	s.Clear()
	assert.Equal(t, uint(0), s.Count())
	foundStorage := [1]V{}
	for _, v := range values {
		ok := s.Has(v.hash, v.value)
		assert.False(t, ok)
		found := foundStorage[:0]
		foundCount := s.Get(v.hash, &found)
		assert.Equal(t, foundCount, uint(0))
	}
	var calls int
	s.Iter(func(h Hash, v V) (stop bool) {
		calls++
		return
	})
	assert.Equal(t, 0, calls)
}

// testSetIter tests iterator of the map.  It starts by adding the complete
// set of values, and then iterates the map and tests stopping after the first
// visit.  A visited map is created using the runtime map type setting the count
// to zero for all storing all values.  Then the full nap is iterated, incrementing
// the visited map count for each value.  The visited map is then checked to ensure
// all values have been visited, and visited only once.
//
// Mutating the map during iteration is then validated by attempting to add the same
// value back into the map during iteration.  This is not expected to change the map
// as the value is known to exist already, so it's unclear how much value this section
// of the test provides.
func testSetIter[V comparable](t *testing.T, values []hashAndValue[V]) {
	s := NewSet[V](uint32(len(values)))
	for _, v := range values {
		s.Put(v.hash, v.value)
	}
	visited := make(map[V]uint, len(values))
	s.Iter(func(h Hash, v V) (stop bool) {
		visited[v] = 0
		stop = true
		return
	})
	if len(values) == 0 {
		assert.Equal(t, len(visited), 0)
	} else {
		assert.Equal(t, len(visited), 1)
	}
	for _, v := range values {
		visited[v.value] = 0
	}
	s.Iter(func(h Hash, v V) (stop bool) {
		visited[v]++
		return
	})
	for _, c := range visited {
		assert.Equal(t, c, uint(1))
	}
	// mutate on iter
	s.Iter(func(h Hash, v V) (stop bool) {
		s.Put(h, v)
		return
	})
	foundStorage := [1]V{}
	for _, v := range values {
		found := foundStorage[:0]
		foundCount := s.Get(v.hash, &found)
		assert.Equal(t, foundCount, uint(1))
		assert.Equal(t, found[0], v.value)
	}
}

// testSetGrow creates a map initially sized to 1/10 of the expected final
// size, then adds all values and confirmes that they can all be retrieved.
func testSetGrow[V comparable](t *testing.T, values []hashAndValue[V]) {
	n := uint32(len(values))
	s := NewSet[V](n / 10)
	for _, v := range values {
		s.Put(v.hash, v.value)
	}
	for _, v := range values {
		found := make([]V, 0, 1)
		foundCount := s.Get(v.hash, &found)
		assert.Equal(t, foundCount, uint(1))
		assert.Equal(t, found[0], v.value)
	}
}

func testSwissSetCapacity[V comparable](t *testing.T, gen func(n int) []hashAndValue[V]) {
	// Capacity() behavior depends on |groupSize|
	// which varies by processor architecture.
	caps := []uint32{
		1 * maxAvgGroupLoad,
		2 * maxAvgGroupLoad,
		3 * maxAvgGroupLoad,
		4 * maxAvgGroupLoad,
		5 * maxAvgGroupLoad,
		10 * maxAvgGroupLoad,
		25 * maxAvgGroupLoad,
		50 * maxAvgGroupLoad,
		100 * maxAvgGroupLoad,
	}
	for _, c := range caps {
		s := NewSet[V](c)
		assert.Equal(t, uint(c), s.Capacity())
		values := gen(rand.Intn(int(c)))
		for _, v := range values {
			s.Put(v.hash, v.value)
		}
		assert.Equal(t, uint(c)-uint(len(values)), s.Capacity())
		assert.Equal(t, uint(c), s.Count()+s.Capacity())
	}
}

func testSetProbeStats[V comparable](t *testing.T, values []hashAndValue[V]) {
	runTest := func(load float32) {
		n := uint32(len(values))
		sz, k := loadFactorSample(n, load)
		s := NewSet[V](sz)
		for _, v := range values[:k] {
			s.Put(v.hash, v.value)
		}
		// todo: assert stat invariants?
		stats := getSetProbeStats[V](t, s, values)
		t.Log(fmtProbeStats(stats))
	}
	t.Run("load_factor=0.5", func(t *testing.T) {
		runTest(0.5)
	})
	t.Run("load_factor=0.75", func(t *testing.T) {
		runTest(0.75)
	})
	t.Run("load_factor=max", func(t *testing.T) {
		runTest(maxLoadFactor)
	})
}

func getSetProbeLength[V comparable](t *testing.T, s *Set[V], hash Hash, value V) (length uint32, ok bool) {
	var end uint32
	hi, _ := splitHash(hash)
	start := probeStart(hi, len(s.groups))
	end, _, ok = s.find(hash, value)
	if end < start { // wrapped
		end += uint32(len(s.groups))
	}
	length = (end - start) + 1
	require.True(t, length > 0)
	return
}

func getSetProbeStats[V comparable](t *testing.T, s *Set[V], values []hashAndValue[V]) (stats probeStats) {
	stats.groups = uint32(len(s.groups))
	stats.loadFactor = s.loadFactor()
	var presentSum, absentSum float32
	stats.presentMin = math.MaxInt32
	stats.absentMin = math.MaxInt32
	for _, v := range values {
		l, ok := getSetProbeLength(t, s, v.hash, v.value)
		if ok {
			stats.presentCnt++
			presentSum += float32(l)
			if stats.presentMin > l {
				stats.presentMin = l
			}
			if stats.presentMax < l {
				stats.presentMax = l
			}
		} else {
			stats.absentCnt++
			absentSum += float32(l)
			if stats.absentMin > l {
				stats.absentMin = l
			}
			if stats.absentMax < l {
				stats.absentMax = l
			}
		}
	}
	if stats.presentCnt == 0 {
		stats.presentMin = 0
	} else {
		stats.presentAvg = presentSum / float32(stats.presentCnt)
	}
	if stats.absentCnt == 0 {
		stats.absentMin = 0
	} else {
		stats.absentAvg = absentSum / float32(stats.absentCnt)
	}
	return
}
