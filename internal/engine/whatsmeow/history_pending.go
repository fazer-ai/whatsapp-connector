package whatsmeow

import (
	"sync"
	"time"
)

// pendingDumps is what keeps two attempts at one dump apart, and the retry to one timer.
type pendingDumps struct {
	mu        sync.Mutex
	handling  map[string]bool
	replaying bool
	again     bool
	armed     bool
	wait      time.Duration
}

func (d *pendingDumps) claim(id string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.handling[id] {
		return false
	}
	if d.handling == nil {
		d.handling = make(map[string]bool)
	}
	d.handling[id] = true
	return true
}

func (d *pendingDumps) release(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.handling, id)
}

// arm reports how long to wait before the next attempt, and whether this caller is the one
// to arm it.
func (d *pendingDumps) arm(first, ceiling time.Duration) (time.Duration, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.armed {
		return 0, false
	}
	d.armed = true
	wait := d.wait
	if wait == 0 {
		wait = first
	}
	d.wait = min(2*wait, ceiling)
	return wait, true
}

func (d *pendingDumps) disarm() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.armed = false
}

func (d *pendingDumps) begin() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.replaying {
		d.again = true
		return false
	}
	d.replaying = true
	return true
}

// end reports whether the replay has to go round again, and otherwise closes it. A replay
// that finished everything starts the next wait from the first one.
func (d *pendingDumps) end(finished bool) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.again {
		d.again = false
		return true
	}
	d.replaying = false
	if finished {
		d.wait = 0
	}
	return false
}
