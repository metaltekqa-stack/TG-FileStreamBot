package stream

import (
	"sync"
	"testing"
	"time"
)

func TestRotationTakesTurns(t *testing.T) {
	now := time.Unix(1000, 0)
	r := newRotation([]any{"a", "b", "c"}, newSlowdowns())
	var got []int
	for range 7 {
		i, wait := r.pick(now)
		if wait != 0 {
			t.Fatalf("waited %v with no bot slowed down", wait)
		}
		got = append(got, i)
	}
	want := []int{0, 1, 2, 0, 1, 2, 0}
	for k := range want {
		if got[k] != want[k] {
			t.Fatalf("turns %v, want %v", got, want)
		}
	}
}

func TestRotationPassesOverSlowedBots(t *testing.T) {
	now := time.Unix(1000, 0)
	slow := newSlowdowns()
	r := newRotation([]any{"a", "b", "c"}, slow)
	slow.set("b", now.Add(5*time.Second))
	for range 6 {
		if i, wait := r.pick(now); i == 1 || wait != 0 {
			t.Fatalf("picked %d (wait %v) while b is slowed down", i, wait)
		}
	}
	// Once its time has passed, b takes its turns again.
	later := now.Add(6 * time.Second)
	seen := map[int]bool{}
	for range 3 {
		i, _ := r.pick(later)
		seen[i] = true
	}
	if !seen[1] {
		t.Fatal("b never came back")
	}
	// An earlier slow down never shortens a later one.
	slow.set("a", now.Add(10*time.Second))
	slow.set("a", now.Add(2*time.Second))
	if got := slow.get("a"); !got.Equal(now.Add(10 * time.Second)) {
		t.Fatalf("slow down shortened to %v", got)
	}
}

func TestRotationWaitsWhenAllAreSlowed(t *testing.T) {
	now := time.Unix(1000, 0)
	slow := newSlowdowns()
	r := newRotation([]any{"a", "b"}, slow)
	slow.set("a", now.Add(8*time.Second))
	slow.set("b", now.Add(3*time.Second))
	i, wait := r.pick(now)
	if i != 1 || wait != 3*time.Second {
		t.Fatalf("picked %d after %v, want b after 3s", i, wait)
	}
	// One bot alone: it waits its own time.
	one := newRotation([]any{"a"}, slow)
	if i, wait := one.pick(now); i != 0 || wait != 8*time.Second {
		t.Fatalf("one bot: %d after %v", i, wait)
	}
	if i, _ := newRotation(nil, slow).pick(now); i != -1 {
		t.Fatal("no bots gave a bot")
	}
}

func TestRotationShared(t *testing.T) {
	// Many streams picking at once, with slow downs being set, stay consistent.
	now := time.Unix(1000, 0)
	slow := newSlowdowns()
	keys := []any{"a", "b", "c", "d"}
	var wg sync.WaitGroup
	for s := range 8 {
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			r := newRotation(keys, slow)
			for k := range 500 {
				i, _ := r.pick(now)
				if i < 0 || i >= len(keys) {
					t.Errorf("picked %d", i)
					return
				}
				if k%50 == 0 {
					slow.set(keys[(s+k)%len(keys)], now.Add(time.Duration(k)*time.Millisecond))
				}
			}
		}(s)
	}
	wg.Wait()
}
