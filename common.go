package swiss

const (
	maxLoadFactor = float32(maxAvgGroupLoad) / float32(groupSize)
)

const (
	h1Mask    uint = 0xffff_ffff_ffff_ff80
	h2Mask    uint = 0x0000_0000_0000_007f
	empty     int8 = -128 // 0b1000_0000
	tombstone int8 = -2   // 0b1111_1110
)

type Hash = uint

// h1 is a 57 bit hash prefix
type h1 = Hash

// h2 is a 7 bit hash suffix
type h2 int8

// metadata is the h2 metadata array for a group.
// find operations first probe the controls bytes
// to filter candidates before matching hashes & values
type metadata [groupSize]int8

func newEmptyMetadata() (meta metadata) {
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

func probeStart(hi h1, groups int) uint32 {
	return fastModN(uint32(hi), uint32(groups))
}

// lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
func fastModN(x, n uint32) uint32 {
	return uint32((uint64(x) * uint64(n)) >> 32)
}

// randIntN returns a random number in the interval [0, n).
func randIntN(n int) uint32 {
	return fastModN(fastrand(), uint32(n))
}
