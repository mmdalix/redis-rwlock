package rwlock

import (
	"context"
	"fmt"
	"time"
)

// Status is a read-only debug snapshot of a resource (SPEC §18).
type Status struct {
	Mode          Mode
	ReaderCount   int
	WriterActive  bool
	QueueLength   int
	QueuedWriters int
	// OldestWait is how long the oldest waiter has waited; -1 if there are no waiters.
	OldestWait time.Duration
	// NextExpiry is the time until the soonest holder lease expires; -1 if no holders.
	NextExpiry time.Duration
}

// Inspect returns a point-in-time snapshot of a resource (read-only).
func (l *RwLock) Inspect(ctx context.Context, resource string) (Status, error) {
	if err := l.ensureReady(ctx); err != nil {
		return Status{}, err
	}
	res, err := l.delivery.run(ctx, "inspect", []string{keyPrefix(resource)}, nil)
	if err != nil {
		return Status{}, fmt.Errorf("%w: inspect %s: %v", ErrBackendUnavailable, resource, err)
	}
	arr, ok := res.([]interface{})
	if !ok || len(arr) < 7 {
		return Status{}, fmt.Errorf("%w: inspect %s: unexpected reply %T", ErrBackendUnavailable, resource, res)
	}
	return Status{
		Mode:          Mode(toStr(arr[0])),
		ReaderCount:   int(toInt64(arr[1])),
		WriterActive:  toInt64(arr[2]) == 1,
		QueueLength:   int(toInt64(arr[3])),
		QueuedWriters: int(toInt64(arr[4])),
		OldestWait:    msDur(toInt64(arr[5])),
		NextExpiry:    msDur(toInt64(arr[6])),
	}, nil
}

func msDur(ms int64) time.Duration {
	if ms < 0 {
		return -1
	}
	return time.Duration(ms) * time.Millisecond
}
