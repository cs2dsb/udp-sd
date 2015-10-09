package util

import (
	"time"
)

func WaitUntilTrue(test func() bool, wait time.Duration) bool {
	w := time.Millisecond * 200
	for wait > 0 {
		if test() {
			return true
		}
		wait -= w
		time.Sleep(w)
	}
	return test()
}