package stream

import (
	"sync"
	"time"
)

// Sharing a stream's blocks between bots. Telegram sets its limits for each bot account, so
// blocks fetched by several bots at once come in faster than from one bot asked for more. A bot
// that Telegram asks to slow down is passed over until the time it gave, and the others carry on;
// only when all of them have been asked to slow down does a block wait. Only the standard library
// here, so it is tested on its own.

// slowdowns is when each bot may be asked again, shared by every stream: a bot told to slow down
// while serving one stream is not asked by the next one either.
type slowdowns struct {
	mu    sync.Mutex
	until map[any]time.Time
}

func newSlowdowns() *slowdowns {
	return &slowdowns{until: map[any]time.Time{}}
}

func (s *slowdowns) set(key any, until time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if until.After(s.until[key]) {
		s.until[key] = until
	}
}

func (s *slowdowns) get(key any) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.until[key]
}

// botSlowdowns holds every bot's slow down, keyed by its client.
var botSlowdowns = newSlowdowns()

// rotation hands out a stream's bots in turn.
type rotation struct {
	mu   sync.Mutex
	keys []any
	next int
	slow *slowdowns
}

func newRotation(keys []any, slow *slowdowns) *rotation {
	return &rotation{keys: keys, slow: slow}
}

// pick is the next bot in turn that has not been asked to slow down (wait 0) or, when all have,
// the one free soonest and how long until it is.
func (r *rotation) pick(now time.Time) (index int, wait time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := len(r.keys)
	if n == 0 {
		return -1, 0
	}
	best, bestAt := -1, time.Time{}
	for k := 0; k < n; k++ {
		i := (r.next + k) % n
		at := r.slow.get(r.keys[i])
		if !at.After(now) {
			r.next = (i + 1) % n
			return i, 0
		}
		if best < 0 || at.Before(bestAt) {
			best, bestAt = i, at
		}
	}
	return best, bestAt.Sub(now)
}
