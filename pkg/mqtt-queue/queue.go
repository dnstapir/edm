package queue

import (
	"container/ring"
	"errors"
	"io"
	"strings"
	"sync"

	"github.com/eclipse/paho.golang/autopaho/queue"
)

var (
	ErrDoublePeek = errors.New("peek was called twice")
	ErrInvalid    = errors.New("invalid state")
)

type RingQueue struct {
	// lock for struct
	lock sync.Mutex

	// stores the current wait channel
	wait chan struct{}

	// write and read positions of the ring
	write *ring.Ring
	read  *ring.Ring

	// reference and status of peeked entry
	peek     *entry
	peekable bool
}

type entry struct {
	queue   *RingQueue
	payload string
}

// A ring buffer that implements the interface of autopaho's
// queue.Queue. The queue may not be copied after its creation.
//
// The queue has n-1 slots for entries, and the "oldest" entry
// is overwritten when the buffer becomes full. Also the entry's
// "Leave" action is intentionally lossy. If n is less than 4,
// then n is set to 4.
func NewRingQueue(n int) *RingQueue {
	if n < 4 {
		n = 4
	}
	r := ring.New(n)
	return &RingQueue{
		wait:     make(chan struct{}),
		write:    r,
		read:     r,
		peek:     nil,
		peekable: true,
	}
}

func (r *RingQueue) Wait() chan struct{} {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.wait
}

func (r *RingQueue) Enqueue(rd io.Reader) error {
	// copy payload into a string
	var payload strings.Builder
	_, err := io.Copy(&payload, rd)
	if err != nil {
		return err
	}

	// lock if payload was read correctly
	r.lock.Lock()
	defer r.lock.Unlock()

	// if the queue is empty before enqueuing this
	// entry => close the wait channel. We can do
	// that now, since we are behind a lock.
	if r.peek == nil && r.write == r.read {
		close(r.wait)
	}

	// store payload
	r.write.Value = payload.String()

	// advance write location
	r.write = r.write.Next()
	// if we catch up with the reader
	if r.write == r.read {
		// discard the oldest entry by advancing
		// the reader
		r.read.Value = nil
		r.read = r.read.Next()
	}

	return nil
}

func (r *RingQueue) Peek() (queue.Entry, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	// if one is in peek buffer
	if r.peek != nil {
		switch r.peekable {
		case true:
			r.peekable = false
			return r.peek, nil
		case false:
			return nil, ErrDoublePeek
		}
	}

	// if queue is empty
	if r.read == r.write {
		return nil, queue.ErrEmpty
	}

	// if there is an entry in the queue
	// => move it to peek
	payload, ok := r.read.Value.(string)
	// if we can't interpret the value as a string
	if !ok {
		// => advance the queue and discard the bad payload
		r.read.Value = nil
		r.read = r.read.Next()
		// return error
		return nil, ErrInvalid
	}
	r.peek = &entry{r, payload}
	// and advance the queue
	r.read.Value = nil
	r.read = r.read.Next()
	// return peek
	r.peekable = false
	return r.peek, nil
}

func (e *entry) Reader() (io.Reader, error) {
	return strings.NewReader(e.payload), nil
}

// the caller needs to have a lock on e.queue.lock
func (e *entry) discard() error {
	// handle non peeked entry and double/incorrect discards
	if e.queue.peekable || e.queue.peek != e {
		return ErrInvalid
	}

	// reset peek
	e.queue.peek = nil
	e.queue.peekable = true

	// if queue is empty
	if e.queue.read == e.queue.write {
		// open new chan
		e.queue.wait = make(chan struct{})
	}
	return nil
}

func (e *entry) Remove() error {
	e.queue.lock.Lock()
	defer e.queue.lock.Unlock()
	return e.discard()
}

func (e *entry) Quarantine() error {
	e.queue.lock.Lock()
	defer e.queue.lock.Unlock()
	return e.discard()
}

// Note: entry might be discarded instead of requeued
func (e *entry) Leave() error {
	e.queue.lock.Lock()
	defer e.queue.lock.Unlock()

	// if write is just behind us, ie the queue is full
	if e.queue.write == e.queue.read.Prev() {
		// => discard peek
		return e.discard()
	}

	// handle non peeked entry and double/incorrect leaves
	if e.queue.peekable || e.queue.peek != e {
		return ErrInvalid
	}

	// make peek peekable again
	e.queue.peekable = true
	return nil
}
