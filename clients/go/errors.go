package rwlock

import "errors"

// Error taxonomy (SPEC §14). Use errors.Is to distinguish; backend failures wrap the
// underlying go-redis error with %w.
var (
	// ErrWaitTimeout: could not acquire within the wait budget. Often not an error for
	// the caller (it's a normal "someone else has it") — check for it explicitly.
	ErrWaitTimeout = errors.New("rwlock: wait timeout")

	// ErrBackendUnavailable: Redis was unreachable or a command failed. Fail-closed —
	// the acquire did NOT happen.
	ErrBackendUnavailable = errors.New("rwlock: backend unavailable")

	// ErrLockLost: a held lock's lease expired or was taken (extend reported LOST, or the
	// watchdog detected it). Stop touching the protected resource.
	ErrLockLost = errors.New("rwlock: lock lost")

	// ErrNotHeld: release/extend on a token not currently held by the caller.
	ErrNotHeld = errors.New("rwlock: not held")

	// ErrIncompatibleServerLogic: the installed module's protocol version is incompatible
	// with this client (SPEC §16).
	ErrIncompatibleServerLogic = errors.New("rwlock: incompatible server protocol version")

	// ErrUnsupported: a required capability is unavailable and no acceptable fallback exists.
	ErrUnsupported = errors.New("rwlock: unsupported capability")

	// ErrClosed: the RwLock has been closed.
	ErrClosed = errors.New("rwlock: closed")
)
