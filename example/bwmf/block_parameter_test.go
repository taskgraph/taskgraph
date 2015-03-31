package bwmf

import (
	"fmt"
	"testing"
)

func reportError(value, actual, expected int, where string, t *testing.T) {
	t.Errorf("%s testing failed. value: %d, actual: %d, expected %d.", where, value, actual, expected)
}

func TestBSearch(t *testing.T) {
	a := []int{0, 100, 200, 300, 400, 500, 520}
	v := []int{0, 10, 100, 101, 200, 299, 300, 399, 400, 499, 500}
	r := []int{0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5}

	fmt.Println("Testing binary search of blockId...")
	for i, _ := range v {
		ra, err := bsearch(a, v[i])
		if ra != r[i] || err != nil {
			reportError(v[i], ra, r[i], "BSearch", t)
		}
	}
	ra, err := bsearch(a, 520)
	if err == nil {
		reportError(520, ra, -1, "BSearch", t)
	}

	b := a[0:2]
	ra2, err2 := bsearch(b, 10)
	if ra2 != 0 || err2 != nil {
		reportError(10, ra2, 0, "BSearch", t)
	}
	fmt.Println("BSearching of blockId Done.")
}
