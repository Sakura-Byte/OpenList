package asmrone

import (
	"context"
	"errors"
	"sync"
	"time"
)

type apiJob struct {
	ctx    context.Context
	run    func() error
	result chan error
	high   bool
}
type apiScheduler struct {
	mu            sync.Mutex
	jobs          []*apiJob
	wake          chan struct{}
	stop          chan struct{}
	done          chan struct{}
	rates         map[*ASMRONE]time.Duration
	next          time.Time
	foregroundRun int
}

var schedulers = struct {
	sync.Mutex
	byHost map[string]*apiScheduler
}{byHost: map[string]*apiScheduler{}}

func acquireScheduler(host string, d *ASMRONE, rps float64) *apiScheduler {
	schedulers.Lock()
	defer schedulers.Unlock()
	s := schedulers.byHost[host]
	if s == nil {
		s = &apiScheduler{wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{}), rates: map[*ASMRONE]time.Duration{}}
		schedulers.byHost[host] = s
		go s.loop()
	}
	s.mu.Lock()
	s.rates[d] = time.Duration(float64(time.Second) / rps)
	s.mu.Unlock()
	return s
}
func releaseScheduler(host string, d *ASMRONE) {
	schedulers.Lock()
	defer schedulers.Unlock()
	s := schedulers.byHost[host]
	if s == nil {
		return
	}
	s.mu.Lock()
	delete(s.rates, d)
	empty := len(s.rates) == 0
	s.mu.Unlock()
	if empty {
		delete(schedulers.byHost, host)
		close(s.stop)
		<-s.done
	}
}
func (s *apiScheduler) do(ctx context.Context, high bool, run func() error) error {
	j := &apiJob{ctx: ctx, run: run, result: make(chan error, 1), high: high}
	s.mu.Lock()
	s.jobs = append(s.jobs, j)
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
	select {
	case err := <-j.result:
		return err
	case <-ctx.Done():
		// Remove queued work immediately. For an active request, wait until
		// cancellation has finished before the caller reads captured results.
		s.mu.Lock()
		for i, pending := range s.jobs {
			if pending == j {
				s.jobs = append(s.jobs[:i], s.jobs[i+1:]...)
				s.mu.Unlock()
				return ctx.Err()
			}
		}
		s.mu.Unlock()
		return <-j.result
	case <-s.stop:
		return errors.New("ASMR.ONE scheduler stopped")
	}
}

func (s *apiScheduler) cooldown(delay time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if until := time.Now().Add(delay); until.After(s.next) {
		s.next = until
	}
}
func (s *apiScheduler) loop() {
	defer close(s.done)
	for {
		s.mu.Lock()
		if len(s.jobs) == 0 {
			s.mu.Unlock()
			select {
			case <-s.wake:
				continue
			case <-s.stop:
				return
			}
		}
		wait := time.Until(s.next)
		s.mu.Unlock()
		if wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-timer.C:
			case <-s.stop:
				timer.Stop()
				return
			}
		}
		s.mu.Lock()
		index := 0
		if len(s.jobs) == 0 || time.Until(s.next) > 0 {
			s.mu.Unlock()
			continue
		}
		if s.foregroundRun < 8 {
			for i, j := range s.jobs {
				if j.high {
					index = i
					break
				}
			}
		} else {
			for i, j := range s.jobs {
				if !j.high {
					index = i
					break
				}
			}
		}
		j := s.jobs[index]
		s.jobs = append(s.jobs[:index], s.jobs[index+1:]...)
		if j.high {
			s.foregroundRun++
		} else {
			s.foregroundRun = 0
		}
		interval := time.Duration(0)
		for _, v := range s.rates {
			if v > interval {
				interval = v
			}
		}
		if j.ctx.Err() == nil {
			s.next = time.Now().Add(interval)
		}
		s.mu.Unlock()
		if err := j.ctx.Err(); err != nil {
			j.result <- err
			continue
		}
		j.result <- j.run()
	}
}
