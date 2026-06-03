package rwlock_test

import (
	"context"
	"errors"
	"testing"
	"time"

	rwlock "github.com/mmdalix/redis-rwlock/clients/go"
	"github.com/mmdalix/redis-rwlock/clients/go/internal/redistest"
	"github.com/redis/go-redis/v9"
)

// threeLocks returns three RwLocks on independent clients to one shared server.
func threeLocks(t *testing.T) (a, b, c *rwlock.RwLock) {
	t.Helper()
	addr := redistest.Start(t).Options().Addr
	mk := func() *rwlock.RwLock {
		l := rwlock.New(redis.NewClient(&redis.Options{Addr: addr}))
		t.Cleanup(func() { _ = l.Close() })
		return l
	}
	return mk(), mk(), mk()
}

// write_preferring (default): a queued writer blocks a new reader from jumping ahead,
// and the writer is served once the holding reader releases (no starvation).
func TestWritePreferringBlocksNewReaderAndServesWriter(t *testing.T) {
	ctx := context.Background()
	reader1, writer, reader2 := threeLocks(t)

	h1, err := reader1.AcquireRead(ctx, "p", rwlock.Lease(30*time.Second))
	if err != nil {
		t.Fatalf("reader1: %v", err)
	}

	wErr := make(chan error, 1)
	whCh := make(chan *rwlock.Handle, 1)
	go func() {
		h, err := writer.AcquireWrite(ctx, "p", rwlock.Lease(30*time.Second), rwlock.Wait(5*time.Second))
		whCh <- h
		wErr <- err
	}()
	time.Sleep(150 * time.Millisecond)

	// a new reader must NOT jump the queued writer -> times out
	if _, err := reader2.AcquireRead(ctx, "p", rwlock.Lease(30*time.Second), rwlock.Wait(300*time.Millisecond)); !errors.Is(err, rwlock.ErrWaitTimeout) {
		t.Fatalf("reader2 = %v, want ErrWaitTimeout", err)
	}

	// release the holding reader -> the writer is served (not starved)
	if err := h1.Release(ctx); err != nil {
		t.Fatalf("release reader1: %v", err)
	}
	if err := <-wErr; err != nil {
		t.Fatalf("writer: %v", err)
	}
	wh := <-whCh
	if wh.Mode != rwlock.ModeWrite {
		t.Fatalf("writer mode = %q", wh.Mode)
	}
	_ = wh.Release(ctx)
}

// read_preferring: a new reader is granted immediately even while a writer is queued.
func TestReadPreferringLetsNewReaderJumpQueuedWriter(t *testing.T) {
	ctx := context.Background()
	reader1, writer, reader2 := threeLocks(t)

	opt := []rwlock.AcquireOption{rwlock.Policy(rwlock.ReadPreferring), rwlock.Lease(30 * time.Second)}

	h1, err := reader1.AcquireRead(ctx, "rp", opt...)
	if err != nil {
		t.Fatalf("reader1: %v", err)
	}
	wErr := make(chan error, 1)
	go func() {
		_, err := writer.AcquireWrite(ctx, "rp", append(opt, rwlock.Wait(5*time.Second))...)
		wErr <- err
	}()
	time.Sleep(150 * time.Millisecond)

	// under read_preferring this is granted right away despite the queued writer
	h2, err := reader2.AcquireRead(ctx, "rp", append(opt, rwlock.Wait(time.Second))...)
	if err != nil {
		t.Fatalf("reader2 (read_preferring) = %v, want granted", err)
	}
	if h2.Mode != rwlock.ModeRead {
		t.Fatalf("reader2 mode = %q", h2.Mode)
	}
	_ = h1.Release(ctx)
	_ = h2.Release(ctx)
	if err := <-wErr; err != nil {
		t.Fatalf("writer eventually: %v", err)
	}
}
