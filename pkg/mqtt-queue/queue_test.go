package queue

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/eclipse/paho.golang/autopaho/queue"
)

func noErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("Error not expected: %v", err)
	}
}

func mustErr(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("Error expected but got nil")
	}
}

func TestRingQueue(t *testing.T) {
	t.Run("implements interface", func(t *testing.T) {
		t.Run("RingQueue", func(t *testing.T) {
			ifs := reflect.TypeFor[queue.Queue]()
			if !reflect.TypeFor[*RingQueue]().Implements(ifs) {
				t.Error("'RingQueue' does not implement autopaho's queue.Queue inteface")
			}
		})

		t.Run("entry", func(t *testing.T) {
			ifs := reflect.TypeFor[queue.Entry]()
			if !reflect.TypeFor[*entry]().Implements(ifs) {
				t.Error("'entry' does not implement autopaho's queue.Entry inteface")
			}
		})
	})

	t.Run("queue lifetime", func(t *testing.T) {
		q := NewRingQueue(4)

		// wait on empty
		wait := q.Wait()
		queueIsEmpty(t, wait)

		// peek on empty
		_, err := q.Peek()
		mustErr(t, err)

		// wait on non empty
		noErr(t, q.Enqueue(strings.NewReader("")))

		queueNotEmpty(t, wait)
		queueNotEmpty(t, q.Wait())

		// peek -> leave
		e, err := q.Peek()
		noErr(t, err)

		queueNotEmpty(t, wait)
		queueNotEmpty(t, q.Wait())

		noErr(t, e.Leave())

		queueNotEmpty(t, wait)
		queueNotEmpty(t, q.Wait())

		// peek -> remove
		e, err = q.Peek()
		noErr(t, err)

		queueNotEmpty(t, wait)
		queueNotEmpty(t, q.Wait())

		noErr(t, e.Remove())

		// old wait will of course stay closed, but a new
		// call to q.Wait() yields an open channel
		queueNotEmpty(t, wait)
		queueIsEmpty(t, q.Wait())

		// enqueue -> peek -> quarantine
		noErr(t, q.Enqueue(strings.NewReader("")))

		e, err = q.Peek()
		noErr(t, err)

		queueNotEmpty(t, q.Wait())

		noErr(t, e.Quarantine())

		queueIsEmpty(t, q.Wait())

		// enqueue bad reader
		mustErr(t, q.Enqueue(iotest.ErrReader(errors.New("test error"))))

		queueIsEmpty(t, q.Wait())
	})

	t.Run("queue order", func(t *testing.T) {
		q := NewRingQueue(4)

		msgs := []string{"A", "B", "C"}

		// enqueue full
		for _, msg := range msgs {
			noErr(t, q.Enqueue(strings.NewReader(msg)))
		}

		// dequeue with peek -> leave -> peek+reader -> remove
		for _, msg := range msgs {
			// peek
			e, err := q.Peek()
			noErr(t, err)

			// leave
			noErr(t, e.Leave())

			// peek and check payload
			e = mustBeElement(t, q, msg)

			// remove
			noErr(t, e.Remove())
		}

		// queue is empty at end
		queueIsEmpty(t, q.Wait())
	})

	t.Run("overwrites", func(t *testing.T) {
		q := NewRingQueue(4)

		msgs := []string{"A", "B", "C", "D", "E"}

		// enqueue overflow
		for _, msg := range msgs {
			noErr(t, q.Enqueue(strings.NewReader(msg)))
		}

		// dequeue
		for _, msg := range msgs[2:] {
			// peek and check payload
			e := mustBeElement(t, q, msg)

			// remove
			noErr(t, e.Remove())
		}

		// queue is empty at end
		queueIsEmpty(t, q.Wait())
	})

	t.Run("overwritten while peeking", func(t *testing.T) {
		q := NewRingQueue(4)

		msgs := []string{"A", "B", "C", "D", "E"}

		// enqueue overflow
		for _, msg := range msgs {
			noErr(t, q.Enqueue(strings.NewReader(msg)))
		}

		// peek and check payload of "C"
		C := mustBeElement(t, q, "C")

		// enqueue new that would have overwritten "C"
		noErr(t, q.Enqueue(strings.NewReader("test")))

		// check that C still contains "C"
		r, err := C.Reader()
		noErr(t, err)
		noErr(t, iotest.TestReader(r, []byte("C")))

		// peek should return error (double peeking is not allowed)
		_, err = q.Peek()
		mustErr(t, err)

		// leave "C", but it gets removed since queue is full
		noErr(t, C.Leave())

		// a second Remove, Quarantine, or Leave action should return error
		mustErr(t, C.Remove())
		mustErr(t, C.Quarantine())
		mustErr(t, C.Leave())

		// peek and check payload of "D"
		D := mustBeElement(t, q, "D")

		// (same as above) should not impact queue
		mustErr(t, C.Remove())
		mustErr(t, C.Quarantine())
		mustErr(t, C.Leave())

		// leave "D"
		noErr(t, D.Leave())

		// (same as above) should not impact queue
		mustErr(t, C.Remove())
		mustErr(t, C.Quarantine())
		mustErr(t, C.Leave())

		// neither should an extra remove for "D"
		mustErr(t, D.Remove())

		// dequeue remainder
		for _, msg := range []string{"D", "E", "test"} {
			// peek and check payload
			e := mustBeElement(t, q, msg)

			// remove
			noErr(t, e.Remove())
		}

		// queue is empty at end
		queueIsEmpty(t, q.Wait())
	})

	// depends on RingQueue's internals
	t.Run("queue length", func(t *testing.T) {
		if q := NewRingQueue(3); q.read.Len() != 4 {
			t.Fatalf("Expected a queue length of 4 got %d", q.read.Len())
		}
		if q := NewRingQueue(4); q.read.Len() != 4 {
			t.Fatalf("Expected a queue length of 4 got %d", q.read.Len())
		}
		if q := NewRingQueue(5); q.read.Len() != 5 {
			t.Fatalf("Expected a queue length of 5 got %d", q.read.Len())
		}
	})

	// depends on RingQueue's internals
	t.Run("invalid payload", func(t *testing.T) {
		q := NewRingQueue(4)
		// enqueue 2 entries
		noErr(t, q.Enqueue(strings.NewReader("A")))
		noErr(t, q.Enqueue(strings.NewReader("B")))
		// change the payload of "A"
		q.read.Value = 0
		// try peeking "A"
		_, err := q.Peek()
		mustErr(t, err)
		// check "B"
		B := mustBeElement(t, q, "B")
		noErr(t, B.Remove())
		// queue is empty at end
		queueIsEmpty(t, q.Wait())
	})
}

func mustBeElement(t *testing.T, q *RingQueue, msg string) queue.Entry {
	t.Helper()

	// peek
	e, err := q.Peek()
	noErr(t, err)

	// payload
	r, err := e.Reader()
	noErr(t, err)
	noErr(t, iotest.TestReader(r, []byte(msg)))

	return e
}

func queueIsEmpty(t *testing.T, ch chan struct{}) {
	t.Helper()

	select {
	case _, ok := <-ch:
		switch ok {
		case true:
			t.Fatal("received item on channel, not allowed")
		case false:
			t.Fatal("wait channel is closed even if queue is empty")
		}
	default:
		// the expected path
	}
}

func queueNotEmpty(t *testing.T, ch chan struct{}) {
	t.Helper()

	select {
	case _, ok := <-ch:
		switch ok {
		case true:
			t.Fatal("received item on channel, not allowed")
		case false:
			// the expected path
		}
	default:
		t.Fatal("wait channel is open even if queue isn't empty")
	}
}
