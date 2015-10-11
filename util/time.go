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

func WaitUntilTrueWithVariableWait(test func() bool, wait func() time.Duration) bool {
	start := time.Now()
	var elapsed time.Duration
	for elapsed < wait() {
		if test() {
			return true
		}
		time.Sleep(time.Millisecond * 200)
		elapsed = time.Now().Sub(start)
	}
	return test()
}