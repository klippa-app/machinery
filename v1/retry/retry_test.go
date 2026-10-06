package retry_test

import (
	"testing"

	"github.com/RichardKnop/machinery/v1/retry"
)

func TestClosure(t *testing.T) {
	closureFunc := retry.Closure()

	stopChan := make(chan int, 1)

	// First call: retryIn starts at 0 so no waiting happens, it just
	// primes retryIn via the fibonacci sequence.
	closureFunc(stopChan)

	// Second call: retryIn is now > 0, so it waits on either the stop
	// channel or a timer. Pre-filling the stop channel lets the select
	// return immediately via the stopChan case.
	stopChan <- 1
	closureFunc(stopChan)

	// Third call: retryIn is 1 again (fibonacci's second value), and this
	// time nothing is sent on stopChan, so the select falls through to the
	// timer branch instead.
	closureFunc(make(chan int))
}
