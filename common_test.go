package swiss

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNumGroups(t *testing.T) {
	assert.Equal(t, expected(0), numGroups(0))
	assert.Equal(t, expected(1), numGroups(1))
	// max load factor 0.875
	assert.Equal(t, expected(14), numGroups(14))
	assert.Equal(t, expected(15), numGroups(15))
	assert.Equal(t, expected(28), numGroups(28))
	assert.Equal(t, expected(29), numGroups(29))
	assert.Equal(t, expected(56), numGroups(56))
	assert.Equal(t, expected(57), numGroups(57))
}

func expected(x int) (groups uint32) {
	groups = uint32(math.Ceil(float64(x) / float64(maxAvgGroupLoad)))
	if groups == 0 {
		groups = 1
	}
	return
}

// calculates the sample size and map size necessary to
// create a load factor of |load| given |n| data points
func loadFactorSample(n uint32, targetLoad float32) (mapSz, sampleSz uint32) {
	if targetLoad > maxLoadFactor {
		targetLoad = maxLoadFactor
	}
	// tables are assumed to be power of two
	sampleSz = uint32(float32(n) * targetLoad)
	mapSz = uint32(float32(n) * maxLoadFactor)
	return
}

type probeStats struct {
	groups     uint32
	loadFactor float32
	presentCnt uint32
	presentMin uint32
	presentMax uint32
	presentAvg float32
	absentCnt  uint32
	absentMin  uint32
	absentMax  uint32
	absentAvg  float32
}

func fmtProbeStats(s probeStats) string {
	g := fmt.Sprintf("groups=%d load=%f\n", s.groups, s.loadFactor)
	p := fmt.Sprintf("present(n=%d): min=%d max=%d avg=%f\n",
		s.presentCnt, s.presentMin, s.presentMax, s.presentAvg)
	a := fmt.Sprintf("absent(n=%d):  min=%d max=%d avg=%f\n",
		s.absentCnt, s.absentMin, s.absentMax, s.absentAvg)
	return g + p + a
}

func mean(samples []float64) (m float64) {
	for _, s := range samples {
		m += s
	}
	return m / float64(len(samples))
}

// uniq takes a slice of values and returns the unique values.  It identifies uniqueness
// using a map with the value as the key and no associated value.  Hashes are not checked
// for uniqueness.
func uniq[V comparable](values []V) []V {
	s := make(map[V]struct{}, len(values))
	for _, v := range values {
		s[v] = struct{}{}
	}
	u := make([]V, 0, len(values))
	for v := range s {
		u = append(u, v)
	}
	return u
}
