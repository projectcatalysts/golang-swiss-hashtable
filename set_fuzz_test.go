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
	"testing"

	"github.com/stretchr/testify/assert"
)

func FuzzStringSet(f *testing.F) {
	f.Add(uint8(1), 14, 50)
	f.Add(uint8(2), 1, 1)
	f.Add(uint8(2), 14, 14)
	f.Add(uint8(2), 14, 15)
	f.Add(uint8(2), 25, 100)
	f.Add(uint8(2), 25, 1000)
	f.Add(uint8(8), 0, 1)
	f.Add(uint8(8), 1, 1)
	f.Add(uint8(8), 14, 14)
	f.Add(uint8(8), 14, 15)
	f.Add(uint8(8), 25, 100)
	f.Add(uint8(8), 25, 1000)
	f.Fuzz(func(t *testing.T, keySz uint8, init, count int) {
		// smaller key sizes generate more overwrites
		fuzzTestStringSet(t, uint32(keySz), uint32(init), uint32(count))
	})
}

func fuzzTestStringSet(t *testing.T, keySz, init, count uint32) {
	const limit = 1024 * 1024
	if count > limit || init > limit {
		t.Skip()
	}
	m := NewSet[string](init)
	if count == 0 {
		return
	}

	values := genStringSetData(int(keySz), int(count), hash64_String)
	if uint(len(values)) != uint(len(uniq(values))) {
		t.Skip()
	}
	golden := make(map[Hash]string, init)
	for _, v := range values {
		m.Put(v.hash, v.value)
		golden[v.hash] = v.value
	}
	assert.Equal(t, uint(len(golden)), m.Count())

	foundStorage := [1]string{}
	for h, v := range golden {
		found := foundStorage[:0]
		foundCount := m.Get(h, &found)
		assert.Equal(t, uint(1), foundCount)
		assert.Equal(t, v, found[0])
	}
	for _, v := range values {
		_, ok := golden[v.hash]
		assert.True(t, ok)
		assert.True(t, m.Has(v.hash, v.value))
	}

	deletes := values[:count/2]
	for _, v := range deletes {
		delete(golden, v.hash)
		m.Delete(v.hash, v.value)
	}
	assert.Equal(t, uint(len(golden)), m.Count())

	for _, v := range deletes {
		assert.False(t, m.Has(v.hash, v.value))
	}
	for h, v := range golden {
		found := foundStorage[:0]
		foundCount := m.Get(h, &found)
		assert.Equal(t, uint(1), foundCount)
		assert.Equal(t, v, found[0])
	}
}
