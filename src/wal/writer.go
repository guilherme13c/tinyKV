// Package wal implements a Write-Ahead-Log
package wal

import (
	"encoding/binary"
	"os"
	"sync"
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

type LogWriter struct {
	file         *os.File
	reqChan      chan *writeRequest
	doneChan     chan struct{}
	wg           sync.WaitGroup
	syncInterval time.Duration
}

func NewWriter(path string) (*LogWriter, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}

	lw := &LogWriter{
		file:         f,
		reqChan:      make(chan *writeRequest, 1024),
		doneChan:     make(chan struct{}),
		syncInterval: DefaultSyncInterval,
	}

	lw.wg.Add(1)
	go lw.runFlusher()

	return lw, nil
}

func (lw *LogWriter) Append(key []byte, value []byte, isTombstone bool) error {
	req := &writeRequest{
		key:         key,
		value:       value,
		isTombstone: isTombstone,
		errChan:     make(chan error, 1),
	}

	select {
	case lw.reqChan <- req:
		// Wait for the result, but also watch for Close so we don't deadlock
		// if the goroutine exits before it processes this request.
		select {
		case err := <-req.errChan:
			return err
		case <-lw.doneChan:
			return os.ErrClosed
		}
	case <-lw.doneChan:
		return os.ErrClosed
	}
}

func (lw *LogWriter) Close() error {
	close(lw.doneChan)
	lw.wg.Wait()
	if err := lw.file.Sync(); err != nil {
		return err
	}
	return lw.file.Close()
}

func (lw *LogWriter) runFlusher() {
	defer lw.wg.Done()

	batch := make([]*writeRequest, 0, 1024)
	// Pre-allocated scratch buffer for batch writes (Fix 2: single Write per batch).
	writeBuf := make([]byte, 0, 64*1024)

	// Fix 1: sync on a ticker instead of after every batch.
	syncTicker := time.NewTicker(lw.syncInterval)
	defer syncTicker.Stop()

	for {
		select {
		case <-lw.doneChan:
			// Drain any requests that were queued before the close signal.
			for {
				select {
				case req := <-lw.reqChan:
					req.errChan <- os.ErrClosed
				default:
					return
				}
			}

		case <-syncTicker.C:
			// Periodic durability flush — push OS page-cache data to disk.
			_ = lw.file.Sync()

		case req := <-lw.reqChan:
			batch = append(batch, req)

		drainLoop:
			for len(batch) < 1024 {
				select {
				case nextReq := <-lw.reqChan:
					batch = append(batch, nextReq)
				default:
					break drainLoop
				}
			}

			// Fix 2: serialise all records into one buffer and issue a
			// single Write() syscall for the entire batch.
			writeBuf = writeBuf[:0]
			for _, r := range batch {
				writeBuf = appendRecord(writeBuf, r)
			}

			var writeErr error
			if _, err := lw.file.Write(writeBuf); err != nil {
				writeErr = err
			}

			// Notify callers: data is in the OS page cache.
			// The sync ticker will flush it to durable storage.
			for _, r := range batch {
				r.errChan <- writeErr
			}

			batch = batch[:0]
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
