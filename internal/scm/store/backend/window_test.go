package backend

import (
	"reflect"
	"testing"
)

// Window takes, of a window looked through in order, up to limit of those to be taken, and says
// where the next window goes on from: after the last looked at, which is the limit-th taken when
// the limit came first.
func TestWindow(t *testing.T) {
	for _, tc := range []struct {
		keys        []int
		take        []bool
		scan, limit int
		taken       []int
		last        int
		looked      int
		more        bool
	}{
		// The window full: more may follow.
		{[]int{1, 2, 3, 4}, []bool{false, true, false, true}, 4, 10, []int{2, 4}, 4, 4, true},
		// Short of the scan: the tail's end.
		{[]int{1, 2, 3, 4}, []bool{false, true, false, true}, 5, 10, []int{2, 4}, 4, 4, false},
		// The limit first, rows after it: it stops there.
		{[]int{1, 2, 3, 4}, []bool{true, true, false, true}, 5, 2, []int{1, 2}, 2, 2, true},
		// The limit at the tail's last row: the end.
		{[]int{1, 2}, []bool{false, true}, 5, 1, []int{2}, 2, 2, false},
		// The limit at a full window's last row: more may follow.
		{[]int{1, 2}, []bool{false, true}, 2, 1, []int{2}, 2, 2, true},
		// Nothing to take, at the end and short of it.
		{[]int{1, 3}, []bool{false, false}, 5, 5, nil, 3, 2, false},
		{[]int{1, 3, 6}, []bool{false, false, false}, 3, 5, nil, 6, 3, true},
	} {
		taken, last, looked, more := Window(tc.keys, tc.take, tc.scan, tc.limit)
		if !reflect.DeepEqual(taken, tc.taken) || last != tc.last || looked != tc.looked || more != tc.more {
			t.Errorf("Window(%v, %v, scan %d, limit %d) = %v, %d, %d, %v; want %v, %d, %d, %v", tc.keys, tc.take, tc.scan, tc.limit,
				taken, last, looked, more, tc.taken, tc.last, tc.looked, tc.more)
		}
	}
}
