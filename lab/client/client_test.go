package main

import (
	"math/rand"
	"testing"
)

// newTestClient builds a client with only the workload fields populated,
// enough to exercise the conflict-rate key generator without any network.
func newTestClient(keyspace, hotKeys, conflictPct int) *client {
	return &client{cfg: &config{
		keyspace:    keyspace,
		hotKeys:     hotKeys,
		conflictPct: conflictPct,
	}}
}

// TestPickKeyConflictRate100 verifies that at a 100% conflict rate every
// request targets a hot key.
func TestPickKeyConflictRate100(t *testing.T) {
	c := newTestClient(1000, 10, 100)
	r := rand.New(rand.NewSource(1))
	const n = 20000
	hot := 0
	for i := 0; i < n; i++ {
		key, isHot := c.pickKey(r)
		if !isHot {
			t.Fatalf("conflict=100: request %d not hot (key=%d)", i, key)
		}
		if key < 0 || key >= 10 {
			t.Fatalf("conflict=100: hot key %d outside [0,10)", key)
		}
		hot++
	}
	if hot != n {
		t.Fatalf("conflict=100: expected %d hot, got %d", n, hot)
	}
}

// TestPickKeyConflictRate0 verifies that at a 0% conflict rate no request
// targets a hot key, and every key is in the cold range.
func TestPickKeyConflictRate0(t *testing.T) {
	c := newTestClient(1000, 10, 0)
	r := rand.New(rand.NewSource(1))
	const n = 20000
	for i := 0; i < n; i++ {
		key, isHot := c.pickKey(r)
		if isHot {
			t.Fatalf("conflict=0: request %d unexpectedly hot (key=%d)", i, key)
		}
		if key < 10 || key >= 1000 {
			t.Fatalf("conflict=0: key %d outside cold range [10,1000)", key)
		}
	}
}

// TestPickKeyConflictRateApprox verifies that the realized hot-key fraction
// is close to the configured conflict percentage. The ranges are disjoint,
// so this is an exact, measurable quantity rather than an RNG assumption.
func TestPickKeyConflictRateApprox(t *testing.T) {
	for _, pct := range []int{10, 25, 50, 75, 90} {
		c := newTestClient(1000, 10, pct)
		r := rand.New(rand.NewSource(int64(pct)))
		const n = 200000
		hot := 0
		for i := 0; i < n; i++ {
			if _, isHot := c.pickKey(r); isHot {
				hot++
			}
		}
		got := 100 * float64(hot) / float64(n)
		if diff := got - float64(pct); diff > 1.0 || diff < -1.0 {
			t.Errorf("conflict=%d: realized %.2f%% outside +/-1%%", pct, got)
		}
	}
}

// TestPickKeyColdRangeNonOverlapping verifies that hot and cold key ranges do
// not overlap, which is what makes the realized conflict rate exact.
func TestPickKeyColdRangeNonOverlapping(t *testing.T) {
	const hot = 10
	c := newTestClient(1000, hot, 50)
	r := rand.New(rand.NewSource(7))
	for i := 0; i < 20000; i++ {
		key, isHot := c.pickKey(r)
		if isHot && key >= hot {
			t.Fatalf("hot key %d >= hot range size %d", key, hot)
		}
		if !isHot && key < hot {
			t.Fatalf("cold key %d inside hot range [0,%d)", key, hot)
		}
	}
}

// TestPickKeyHotKeysEqualsKeyspace verifies the degenerate configuration
// where every key is hot: nothing should panic or divide by zero.
func TestPickKeyHotKeysEqualsKeyspace(t *testing.T) {
	c := newTestClient(10, 10, 25)
	r := rand.New(rand.NewSource(3))
	for i := 0; i < 1000; i++ {
		key, _ := c.pickKey(r)
		if key < 0 || key >= 10 {
			t.Fatalf("key %d outside keyspace [0,10)", key)
		}
	}
}
