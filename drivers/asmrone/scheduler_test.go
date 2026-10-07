package asmrone

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSharedSchedulerRateAndConcurrency(t *testing.T) {
	one, two := &ASMRONE{}, &ASMRONE{}
	s := acquireScheduler("rate-test", one, 1)
	require.Same(t, s, acquireScheduler("rate-test", two, 100))
	defer releaseScheduler("rate-test", two)
	defer releaseScheduler("rate-test", one)
	var mu sync.Mutex
	var starts []time.Time
	active, maximum := 0, 0
	run := func() error {
		mu.Lock()
		starts = append(starts, time.Now())
		active++
		maximum = max(maximum, active)
		mu.Unlock()
		time.Sleep(15 * time.Millisecond)
		mu.Lock()
		active--
		mu.Unlock()
		return nil
	}
	var wg sync.WaitGroup
	for range 3 {
		wg.Go(func() { require.NoError(t, s.do(context.Background(), false, run)) })
	}
	wg.Wait()
	require.Equal(t, 1, maximum)
	require.Len(t, starts, 3)
	for i := 1; i < len(starts); i++ {
		require.GreaterOrEqual(t, starts[i].Sub(starts[i-1]), 990*time.Millisecond)
	}
}

func TestSchedulerPrioritizesForegroundAndCancelsQueuedRequests(t *testing.T) {
	d := &ASMRONE{}
	s := acquireScheduler("priority-test", d, 100)
	defer releaseScheduler("priority-test", d)
	started, release := make(chan struct{}), make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- s.do(context.Background(), false, func() error { close(started); <-release; return nil })
	}()
	<-started
	var mu sync.Mutex
	var order []string
	appendOrder := func(name string) func() error {
		return func() error { mu.Lock(); order = append(order, name); mu.Unlock(); return nil }
	}
	lowDone, highDone := make(chan error, 1), make(chan error, 1)
	go func() { lowDone <- s.do(context.Background(), false, appendOrder("background")) }()
	go func() { highDone <- s.do(context.Background(), true, appendOrder("foreground")) }()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, s.do(canceled, true, appendOrder("canceled")), context.Canceled)
	require.Eventually(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return len(s.jobs) >= 2 }, time.Second, time.Millisecond)
	close(release)
	require.NoError(t, <-firstDone)
	require.NoError(t, <-lowDone)
	require.NoError(t, <-highDone)
	require.Equal(t, []string{"foreground", "background"}, order)
}

func TestRetryAfter(t *testing.T) {
	require.Equal(t, 3*time.Second, retryAfter("3"))
	require.Zero(t, retryAfter("invalid"))
	require.Greater(t, retryAfter(time.Now().Add(time.Minute).UTC().Format(http.TimeFormat)), 50*time.Second)
}
