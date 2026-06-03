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

func newLock(t *testing.T) *rwlock.RwLock {
	t.Helper()
	client := redistest.Start(t)
	l := rwlock.New(client)
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func TestAcquireWriteGrantsWithFencing(t *testing.T) {
	ctx := context.Background()
	l := newLock(t)

	before := time.Now()
	h, err := l.AcquireWrite(ctx, "r")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if h.Mode != rwlock.ModeWrite {
		t.Fatalf("mode = %q, want write", h.Mode)
	}
	if h.Fencing <= 0 {
		t.Fatalf("fencing = %d, want > 0", h.Fencing)
	}
	if !h.LeaseUntil().After(before) {
		t.Fatalf("leaseUntil %v not after %v", h.LeaseUntil(), before)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
}

func TestDeliveryIsFunctionsOnRedis7(t *testing.T) {
	ctx := context.Background()
	l := newLock(t)
	h, err := l.AcquireWrite(ctx, "d")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	_ = h.Release(ctx)
	if got := l.DeliveryMode(); got != "functions" {
		t.Fatalf("delivery mode = %q, want functions", got)
	}
}

func TestWriteExcludesSecondWriterAndTimesOut(t *testing.T) {
	ctx := context.Background()
	l := newLock(t)

	h, err := l.AcquireWrite(ctx, "r", rwlock.Lease(30*time.Second))
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer h.Release(ctx)

	start := time.Now()
	_, err = l.AcquireWrite(ctx, "r", rwlock.Wait(300*time.Millisecond))
	if !errors.Is(err, rwlock.ErrWaitTimeout) {
		t.Fatalf("err = %v, want ErrWaitTimeout", err)
	}
	if d := time.Since(start); d < 250*time.Millisecond || d > 3*time.Second {
		t.Fatalf("timed out after %v, want ~300ms", d)
	}
}

func TestHandoffOnRelease(t *testing.T) {
	ctx := context.Background()
	server := redistest.Start(t)
	addr := server.Options().Addr

	a := rwlock.New(redis.NewClient(&redis.Options{Addr: addr}))
	b := rwlock.New(redis.NewClient(&redis.Options{Addr: addr}))
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })

	h1, err := a.AcquireWrite(ctx, "r", rwlock.Lease(30*time.Second))
	if err != nil {
		t.Fatalf("a acquire: %v", err)
	}

	type res struct {
		h   *rwlock.Handle
		err error
	}
	done := make(chan res, 1)
	go func() {
		h, err := b.AcquireWrite(ctx, "r", rwlock.Lease(30*time.Second), rwlock.Wait(5*time.Second))
		done <- res{h, err}
	}()

	time.Sleep(150 * time.Millisecond)
	start := time.Now()
	if err := h1.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}

	r := <-done
	if r.err != nil {
		t.Fatalf("b acquire: %v", r.err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("handoff took %v, want fast", time.Since(start))
	}
	if r.h.Fencing <= h1.Fencing {
		t.Fatalf("fencing %d not > %d", r.h.Fencing, h1.Fencing)
	}
	_ = r.h.Release(ctx)
}

func TestFencingMonotonic(t *testing.T) {
	ctx := context.Background()
	l := newLock(t)
	var prev int64
	for i := 0; i < 5; i++ {
		h, err := l.AcquireWrite(ctx, "r")
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		if h.Fencing <= prev {
			t.Fatalf("fencing %d not > %d", h.Fencing, prev)
		}
		prev = h.Fencing
		_ = h.Release(ctx)
	}
}

func TestReadersCoHold(t *testing.T) {
	ctx := context.Background()
	l := newLock(t)
	a, err := l.AcquireRead(ctx, "r", rwlock.Lease(30*time.Second))
	if err != nil {
		t.Fatalf("read a: %v", err)
	}
	b, err := l.AcquireRead(ctx, "r", rwlock.Lease(30*time.Second))
	if err != nil {
		t.Fatalf("read b: %v", err)
	}
	if a.Mode != rwlock.ModeRead || b.Mode != rwlock.ModeRead {
		t.Fatalf("modes = %q,%q want read,read", a.Mode, b.Mode)
	}
	if b.Fencing <= a.Fencing {
		t.Fatalf("fencing %d not > %d", b.Fencing, a.Fencing)
	}
	_ = a.Release(ctx)
	_ = b.Release(ctx)
}

func TestExtendRenewsAndReportsLost(t *testing.T) {
	ctx := context.Background()
	l := newLock(t)

	h, err := l.AcquireWrite(ctx, "r", rwlock.Lease(30*time.Second))
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	before := h.LeaseUntil()
	if err := h.Extend(ctx, 60*time.Second); err != nil {
		t.Fatalf("extend: %v", err)
	}
	if !h.LeaseUntil().After(before) {
		t.Fatalf("extend did not push leaseUntil")
	}
	_ = h.Release(ctx)

	h2, err := l.AcquireWrite(ctx, "r2", rwlock.Lease(200*time.Millisecond))
	if err != nil {
		t.Fatalf("acquire short: %v", err)
	}
	time.Sleep(350 * time.Millisecond)
	if err := h2.Extend(ctx, time.Second); !errors.Is(err, rwlock.ErrLockLost) {
		t.Fatalf("extend after expiry = %v, want ErrLockLost", err)
	}
}

func TestFailsClosedWhenRedisUnreachable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	dead := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 200 * time.Millisecond, MaxRetries: -1})
	defer dead.Close()
	l := rwlock.New(dead)
	defer l.Close()

	if _, err := l.AcquireWrite(ctx, "r"); !errors.Is(err, rwlock.ErrBackendUnavailable) {
		t.Fatalf("err = %v, want ErrBackendUnavailable", err)
	}
}
