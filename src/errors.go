package src

import (
	"errors"
	"fmt"
)

// ErrKeyNotFound is a sentinel for use with errors.Is.
var ErrKeyNotFound = errors.New("key not found")

// ErrTombstone is returned by Get when the key exists but is deleted.
// Callers should stop searching older sources when they see this.
var ErrTombstone = errors.New("key is tombstoned")

// KeyNotFoundError carries the offending key.
type KeyNotFoundError struct {
	Key []byte
}

func (e *KeyNotFoundError) Error() string {
	return fmt.Sprintf("key not found: %q", e.Key)
}

// Is makes errors.Is(err, ErrKeyNotFound) return true for KeyNotFoundError.
func (e *KeyNotFoundError) Is(target error) bool {
	return target == ErrKeyNotFound
}
