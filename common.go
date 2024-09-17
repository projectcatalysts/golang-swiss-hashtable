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
// introduced by Project Catalysts under the same Apache License, Version 2.0.
package swiss

const (
	maxLoadFactor = float32(maxAvgGroupLoad) / float32(groupSize)
)

const (
	h1Mask    uint64 = 0xffff_ffff_ffff_ff80
	h2Mask    uint64 = 0x0000_0000_0000_007f
	empty     int8   = -128 // 0b1000_0000
	tombstone int8   = -2   // 0b1111_1110
)

type Hash = uint64

// h1 is a 57 bit hash prefix
type h1 = Hash

// h2 is a 7 bit hash suffix
type h2 int8

// groupMetadata is the h2 metadata array for a group.
// find operations first probe the controls bytes
// to filter candidates before matching hashes & values
type groupMetadata [groupSize]int8
type groupHashes [groupSize]Hash
type groupValues[V comparable] []V

func newEmptyMetadata() (meta groupMetadata) {
	for i := range meta {
		meta[i] = empty
	}
	return
}

var (
	emptyMeta = newEmptyMetadata()
)

// numGroups returns the minimum number of groups needed to store |n| elems.
func numGroups(n uint32) (groups uint32) {
	groups = (n + maxAvgGroupLoad - 1) / maxAvgGroupLoad
	if groups == 0 {
		groups = 1
	}
	return
}

func splitHash(h Hash) (h1, h2) {
	return h1((h & h1Mask) >> 7), h2(h & h2Mask)
}

func probeStart(hi h1, groupCount uint32) uint32 {
	return fastModN(uint32(hi), groupCount)
}

// lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
func fastModN(x, n uint32) uint32 {
	return uint32((uint64(x) * uint64(n)) >> 32)
}

// randIntN returns a random number in the interval [0, n).
func randIntN(n uint32) uint32 {
	return fastModN(fastrand(), n)
}
