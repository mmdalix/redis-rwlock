package rwlock

import "context"

// WithWriteLock acquires the write lock, runs fn while holding it, and releases on
// return (even on error or panic-free early return). The context passed to fn is
// cancelled the instant the lock is lost (with WithExtensionMargin/Watchdog), so
// in-flight work stops; check context.Cause(ctx) == ErrLockLost to tell loss from a
// normal caller cancellation.
func (l *RwLock) WithWriteLock(ctx context.Context, resource string, fn func(context.Context, *Handle) error, opts ...AcquireOption) error {
	_, err := DoWrite(ctx, l, resource, adaptErr(fn), opts...)
	return err
}

// WithReadLock is the read-lock counterpart of WithWriteLock.
func (l *RwLock) WithReadLock(ctx context.Context, resource string, fn func(context.Context, *Handle) error, opts ...AcquireOption) error {
	_, err := DoRead(ctx, l, resource, adaptErr(fn), opts...)
	return err
}

// DoWrite is the generic, value-returning form of WithWriteLock. (Go methods can't take
// type parameters, so this is a package function.)
func DoWrite[T any](ctx context.Context, l *RwLock, resource string, fn func(context.Context, *Handle) (T, error), opts ...AcquireOption) (T, error) {
	var zero T
	h, err := l.AcquireWrite(ctx, resource, opts...)
	if err != nil {
		return zero, err
	}
	return runScoped(ctx, h, fn)
}

// DoRead is the generic, value-returning form of WithReadLock.
func DoRead[T any](ctx context.Context, l *RwLock, resource string, fn func(context.Context, *Handle) (T, error), opts ...AcquireOption) (T, error) {
	var zero T
	h, err := l.AcquireRead(ctx, resource, opts...)
	if err != nil {
		return zero, err
	}
	return runScoped(ctx, h, fn)
}

func runScoped[T any](ctx context.Context, h *Handle, fn func(context.Context, *Handle) (T, error)) (T, error) {
	defer h.Release(context.WithoutCancel(ctx)) // release even if the caller ctx is done

	// fnCtx is cancelled if the caller's ctx is cancelled OR the lock is lost.
	fnCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	go func() {
		select {
		case <-h.Context().Done():
			cancel(context.Cause(h.Context())) // propagate ErrLockLost
		case <-fnCtx.Done():
		}
	}()
	return fn(fnCtx, h)
}

func adaptErr(fn func(context.Context, *Handle) error) func(context.Context, *Handle) (struct{}, error) {
	return func(ctx context.Context, h *Handle) (struct{}, error) { return struct{}{}, fn(ctx, h) }
}
