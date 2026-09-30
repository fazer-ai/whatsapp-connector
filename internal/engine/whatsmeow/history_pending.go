package whatsmeow

import (
	"sync"
	"time"
)

// pendingDumps is what keeps two attempts at one dump apart, and the retry to one timer.
type pendingDumps struct {
	mu       sync.Mutex
	handling map[string]bool
	armed    bool
	wait     time.Duration
	// unwritten is the dumps that arrived while the store would not take them.
	unwritten map[string]unwrittenDump
}

// unwrittenDump is a notification kept in memory until it can be written down.
type unwrittenDump struct {
	id         string
	device     string
	notice     []byte
	learned    int64
	generation uint64
}

func (d *pendingDumps) keepUnwritten(dump unwrittenDump) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.unwritten == nil {
		d.unwritten = make(map[string]unwrittenDump)
	}
	d.unwritten[dump.id] = dump
}

func (d *pendingDumps) forgetUnwritten(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.unwritten, id)
}

// unwrittenDumps is what is kept in memory. In no order: once written, the rows are read
// back oldest first.
func (d *pendingDumps) unwrittenDumps() []unwrittenDump {
	d.mu.Lock()
	defer d.mu.Unlock()
	dumps := make([]unwrittenDump, 0, len(d.unwritten))
	for _, dump := range d.unwritten {
		dumps = append(dumps, dump)
	}
	return dumps
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

// settle starts the next wait from the first one, after a replay that finished everything.
func (d *pendingDumps) settle() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.wait = 0
}
