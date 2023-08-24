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

import (
	"math/bits"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

func BenchmarkStringSets(b *testing.B) {
	const keySz = 8
	sizes := []int{16, 128, 1024, 8192, 131072}
	for _, n := range sizes {
		b.Run("n="+strconv.Itoa(n), func(b *testing.B) {
			b.Run("runtime map", func(b *testing.B) {
				benchmarkRuntimeSet(b, genStringSetData(keySz, n, hash64_String))
			})
			b.Run("swiss.Set(Get)", func(b *testing.B) {
				benchmarkSwissSetGet(b, genStringSetData(keySz, n, hash64_String))
			})
			b.Run("swiss.Set(Has)", func(b *testing.B) {
				benchmarkSwissSetHas(b, genStringSetData(keySz, n, hash64_String))
			})
		})
	}
}

func BenchmarkInt64Sets(b *testing.B) {
	sizes := []int{16, 128, 1024, 8192, 131072}
	for _, n := range sizes {
		b.Run("n="+strconv.Itoa(n), func(b *testing.B) {
			b.Run("runtime map", func(b *testing.B) {
				benchmarkRuntimeMap(b, generateInt64SetData(n, hash64_Int64))
			})
			b.Run("swiss.Map(get)", func(b *testing.B) {
				benchmarkSwissSetGet(b, generateInt64SetData(n, hash64_Int64))
			})
			b.Run("swiss.Map(has)", func(b *testing.B) {
				benchmarkSwissSetHas(b, generateInt64SetData(n, hash64_Int64))
			})
		})
	}
}

func TestSetMemoryFootprint(t *testing.T) {
	t.Skip("unskip for memory footprint stats")
	var samples []float64
	for n := 10; n <= 10_000; n += 10 {
		b1 := testing.Benchmark(func(b *testing.B) {
			// max load factor 7/8
			m := NewSet[int](uint32(n))
			require.NotNil(b, m)
		})
		b2 := testing.Benchmark(func(b *testing.B) {
			// max load factor 6.5/8
			m := make(map[int]int, n)
			require.NotNil(b, m)
		})
		x := float64(b1.MemBytes) / float64(b2.MemBytes)
		samples = append(samples, x)
	}
	t.Logf("mean size ratio: %.3f", mean(samples))
}

func benchmarkRuntimeSet[V comparable](b *testing.B, values []hashAndValue[V]) {
	valueCount := uint32(len(values))
	mod := valueCount - 1 // power of 2 fast modulus
	require.Equal(b, 1, bits.OnesCount32(valueCount))
	m := make(map[Hash]V, valueCount)
	for _, v := range values {
		m[v.hash] = v.value
	}
	b.ResetTimer()
	b.ReportAllocs()
	var ok bool
	for i := 0; i < b.N; i++ {
		_, ok = m[values[uint32(i)&mod].hash]
	}
	assert.True(b, ok)
}

func benchmarkSwissSetGet[V comparable](b *testing.B, values []hashAndValue[V]) {
	valueCount := uint32(len(values))
	mod := valueCount - 1 // power of 2 fast modulus
	require.Equal(b, 1, bits.OnesCount32(valueCount))
	m := NewSet[V](valueCount)
	for _, v := range values {
		m.Put(v.hash, v.value)
	}
	foundStorage := [1]V{}
	b.ResetTimer()
	b.ReportAllocs()
	var foundCount uint
	for i := 0; i < b.N; i++ {
		found := foundStorage[:0]
		foundCount = m.Get(values[uint32(i)&mod].hash, &found)
	}
	assert.Equal(b, uint(1), foundCount)
}

func benchmarkSwissSetHas[V comparable](b *testing.B, values []hashAndValue[V]) {
	valueCount := uint32(len(values))
	mod := valueCount - 1 // power of 2 fast modulus
	require.Equal(b, 1, bits.OnesCount32(valueCount))
	m := NewSet[V](valueCount)
	for _, v := range values {
		m.Put(v.hash, v.value)
	}
	b.ResetTimer()
	b.ReportAllocs()
	var ok bool
	for i := 0; i < b.N; i++ {
		v := values[uint32(i)&mod]
		ok = m.Has(v.hash, v.value)
	}
	assert.True(b, ok)
}
