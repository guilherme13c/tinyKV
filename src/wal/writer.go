// Package wal implements a Write-Ahead-Log
package wal

import (
	"encoding/binary"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultSyncInterval is the interval between periodic WAL syncs.
// Append() returns once data lands in the OS page cache; it is flushed
// to durable storage at most DefaultSyncInterval later.
// Close() always performs a final sync regardless of this interval.
const DefaultSyncInterval = 10 * time.Millisecond

type writeRequest struct {
	key         []byte
	value       []byte
	isTombstone bool
	errChan     chan error
}

// LogWriter uses a write-stealing leader-election scheme.
//
// Every caller of Append enqueues its request and then competes for mu.
// The winner (leader) drains ALL pending requests — including those from
// goroutines that were blocked waiting for mu — serialises them into a
// single buffer, and issues one file.Write syscall for the entire batch.
// Each stolen goroutine's errChan is signalled by the leader, so it can
// return without ever touching the file itself.
//
// Lock ordering: pendingMu is always acquired while mu is held.
// syncLoop only calls file.Sync and never acquires either lock.
type LogWriter struct {
	file     *os.File
	mu       sync.Mutex      // leader-election lock
	pendingMu sync.Mutex     // guards pending slice
	pending  []*writeRequest // queue of waiting writers
	batchBuf []*writeRequest // scratch batch buffer; owned by leader under mu
	writeBuf []byte          // scratch write buffer; owned by leader under mu

	closed   atomic.Bool
	doneChan chan struct{}
	wg       sync.WaitGroup

	syncInterval time.Duration
	reqPool      sync.Pool
}

func NewWriter(path string) (*LogWriter, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}

	lw := &LogWriter{
		file:         f,
		pending:      make([]*writeRequest, 0, 64),
		batchBuf:     make([]*writeRequest, 0, 64),
		writeBuf:     make([]byte, 0, 64*1024),
		doneChan:     make(chan struct{}),
		syncInterval: DefaultSyncInterval,
		reqPool: sync.Pool{
			New: func() any {
				return &writeRequest{errChan: make(chan error, 1)}
			},
		},
	}

	lw.wg.Add(1)
	go lw.syncLoop()

	return lw, nil
}

func (lw *LogWriter) Append(key []byte, value []byte, isTombstone bool) error {
	if lw.closed.Load() {
		return os.ErrClosed
	}

	req := lw.reqPool.Get().(*writeRequest)
	req.key = key
	req.value = value
	req.isTombstone = isTombstone

	// Enqueue BEFORE acquiring mu.  Any leader that drains after this point
	// is guaranteed to see our request — so our errChan will always be
	// signalled exactly once.
	lw.pendingMu.Lock()
	lw.pending = append(lw.pending, req)
	lw.pendingMu.Unlock()

	// Compete to become the leader.  Goroutines that lose wait here; by the
	// time they win, the previous leader may have already processed their
	// request (write-stealing).
	lw.mu.Lock()

	if lw.closed.Load() {
		// Writer was closed while we waited.  Drain pending (including ours)
		// and signal ErrClosed to everyone.
		lw.pendingMu.Lock()
		for _, r := range lw.pending {
			r.errChan <- os.ErrClosed
		}
		lw.pending = lw.pending[:0]
		lw.pendingMu.Unlock()
		lw.mu.Unlock()

		err := <-req.errChan
		lw.reqPool.Put(req)
		return err
	}

	// Drain all pending writers (ours plus any that arrived while we waited).
	lw.pendingMu.Lock()
	lw.batchBuf = append(lw.batchBuf[:0], lw.pending...)
	lw.pending = lw.pending[:0]
	lw.pendingMu.Unlock()

	// batchBuf may be empty if a previous leader already stole our request.
	var writeErr error
	if len(lw.batchBuf) > 0 {
		lw.writeBuf = lw.writeBuf[:0]
		for _, r := range lw.batchBuf {
			lw.writeBuf = appendRecord(lw.writeBuf, r)
		}
		if _, err := lw.file.Write(lw.writeBuf); err != nil {
			writeErr = err
		}
		for _, r := range lw.batchBuf {
			r.errChan <- writeErr
		}
	}

	lw.mu.Unlock()

	err := <-req.errChan
	lw.reqPool.Put(req)
	return err
}

func (lw *LogWriter) Close() error {
	close(lw.doneChan)
	lw.wg.Wait()

	// Acquire the leader lock so that: (a) any in-flight leader finishes
	// before we proceed, and (b) setting closed and draining pending is atomic
	// from the perspective of racing Append callers.
	lw.mu.Lock()
	lw.closed.Store(true)
	lw.pendingMu.Lock()
	for _, r := range lw.pending {
		r.errChan <- os.ErrClosed
	}
	lw.pending = lw.pending[:0]
	lw.pendingMu.Unlock()
	lw.mu.Unlock()

	if err := lw.file.Sync(); err != nil {
		return err
	}
	return lw.file.Close()
}

// syncLoop periodically calls file.Sync to push OS page-cache data to
// durable storage.  It does not hold any write lock, so it never blocks
// concurrent Append calls.
func (lw *LogWriter) syncLoop() {
	defer lw.wg.Done()
	ticker := time.NewTicker(lw.syncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-lw.doneChan:
			return
		case <-ticker.C:
			_ = lw.file.Sync()
		}
	}
}

// appendRecord serialises a single write request into buf and returns the
// extended slice. Layout: [uvarint keyLen | uvarint valueMeta | key | value?]
// where valueMeta = (len(value) << 1) | isTombstone.
func appendRecord(buf []byte, r *writeRequest) []byte {
	var hdr [20]byte // large enough for two uvarints (max 10 bytes each)
	n1 := binary.PutUvarint(hdr[:], uint64(len(r.key)))
	n2 := binary.PutUvarint(hdr[n1:], encodeLength(len(r.value), r.isTombstone))
	buf = append(buf, hdr[:n1+n2]...)
	buf = append(buf, r.key...)
	if !r.isTombstone {
		buf = append(buf, r.value...)
	}
	return buf
}

// encodeLength packs the length and the tombstone flag into a single uint64.
func encodeLength(length int, isTombstone bool) uint64 {
	packed := uint64(length) << 1
	if isTombstone {
		packed |= 1
	}
	return packed
}
