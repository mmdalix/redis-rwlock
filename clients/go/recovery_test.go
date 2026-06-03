package rwlock_test

import (
	"context"
	"testing"
	"time"

	rwlock "github.com/mmdalix/redis-rwlock/clients/go"
	"github.com/mmdalix/redis-rwlock/clients/go/internal/redistest"
	"github.com/redis/go-redis/v9"
)

func TestInspectWriterAndFree(t *testing.T) {
	ctx := context.Background()
	l := newLock(t)

	h, err := l.AcquireWrite(ctx, "i", rwlock.Lease(30*time.Second))
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	s, err := l.Inspect(ctx, "i")
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if s.Mode != rwlock.ModeWrite || !s.WriterActive || s.ReaderCount != 0 {
		t.Fatalf("status = %+v, want write/active", s)
	}
	if s.NextExpiry <= 0 || s.NextExpiry > 30*time.Second {
		t.Fatalf("nextExpiry = %v", s.NextExpiry)
	}
	_ = h.Release(ctx)

	s, _ = l.Inspect(ctx, "i")
	if s.Mode != "none" || s.NextExpiry != -1 {
		t.Fatalf("after release status = %+v, want none/-1", s)
	}
}

func TestInspectReportsQueuedWriter(t *testing.T) {
	ctx := context.Background()
	reader, writer, _ := threeLocks(t)

	h, err := reader.AcquireRead(ctx, "iq", rwlock.Lease(30*time.Second))
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	go func() {
		_, _ = writer.AcquireWrite(ctx, "iq", rwlock.Lease(30*time.Second), rwlock.Wait(3*time.Second))
	}()
	time.Sleep(200 * time.Millisecond)

	s, err := reader.Inspect(ctx, "iq")
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if s.Mode != rwlock.ModeRead || s.ReaderCount != 1 {
		t.Fatalf("status = %+v, want read/1 reader", s)
	}
	if s.QueueLength != 1 || s.QueuedWriters != 1 {
		t.Fatalf("status = %+v, want queueLength=1 queuedWriters=1", s)
	}
	if s.OldestWait < 0 {
		t.Fatalf("oldestWait = %v, want >= 0", s.OldestWait)
	}
	_ = h.Release(ctx)
}

func TestKeyspaceSubscriberReclaimsCrashedWriter(t *testing.T) {
	ctx := context.Background()
	server := redistest.Start(t, "--notify-keyspace-events", "Ex")
	addr := server.Options().Addr

	recovered := make(chan string, 4)
	l := rwlock.New(redis.NewClient(&redis.Options{Addr: addr}),
		rwlock.WithOnRecovery(func(r string) { recovered <- r }))
	t.Cleanup(func() { _ = l.Close() })

	// crash: short lease, never released, no waiter. The writer key self-expires via its
	// native TTL and fires an `expired` event; the subscriber runs expire_and_grant.
	if _, err := l.AcquireWrite(ctx, "orphan", rwlock.Lease(300*time.Millisecond)); err != nil {
		t.Fatalf("acquire: %v", err)
	}

	select {
	case r := <-recovered:
		if r != "orphan" {
			t.Fatalf("recovered %q, want orphan", r)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("keyspace subscriber did not fire onRecovery for the crashed writer")
	}

	s, err := l.Inspect(ctx, "orphan")
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if s.Mode != "none" {
		t.Fatalf("after self-expiry mode = %q, want none", s.Mode)
	}
}
