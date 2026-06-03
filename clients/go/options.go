package rwlock

import (
	"log/slog"
	"time"
)

// Mode is the lock mode.
type Mode string

const (
	ModeRead  Mode = "read"
	ModeWrite Mode = "write"
)

// Fairness selects the queueing policy (SPEC §11).
type Fairness string

const (
	// WritePreferring (default): a queued writer blocks new readers from jumping ahead.
	WritePreferring Fairness = "write_preferring"
	// FIFO: strict queue order (contiguous readers still batch).
	FIFO Fairness = "fifo"
	// ReadPreferring: readers proceed whenever no writer holds; writers may starve.
	ReadPreferring Fairness = "read_preferring"
)

type deliveryPref int

const (
	deliveryAuto deliveryPref = iota
	deliveryFunctions
	deliveryScripts
)

// config holds RwLock-wide settings (SPEC §19).
type config struct {
	defaultLease         time.Duration
	maxLease             time.Duration
	defaultWait          time.Duration
	maxWait              time.Duration
	defaultFairness      Fairness
	maxReaderBatch       int
	notifyKeyTTL         time.Duration
	requestKeyTTLGrace   time.Duration
	requireOwnerID       bool
	blockingPoolSize     int
	extensionMargin      time.Duration
	delivery             deliveryPref
	allowIncompatibleVer bool
	keyspaceEvents       bool
	onRecovery           func(resource string)
	logger               *slog.Logger
}

func defaultConfig() config {
	return config{
		defaultLease:       30 * time.Second,
		maxLease:           300 * time.Second,
		defaultWait:        10 * time.Second,
		maxWait:            60 * time.Second,
		defaultFairness:    WritePreferring,
		maxReaderBatch:     1000,
		notifyKeyTTL:       60 * time.Second,
		requestKeyTTLGrace: 60 * time.Second,
		requireOwnerID:     false,
		blockingPoolSize:   16,
		extensionMargin:    500 * time.Millisecond,
		delivery:           deliveryAuto,
		keyspaceEvents:     true, // auto-detect; no-op if the server has them disabled
		logger:             slog.New(slog.DiscardHandler),
	}
}

// Option configures a RwLock at construction.
type Option func(*config)

// WithDefaultLease sets the default hold duration (per-acquire Lease overrides it).
func WithDefaultLease(d time.Duration) Option { return func(c *config) { c.defaultLease = d } }

// WithDefaultWait sets the default time to block waiting.
func WithDefaultWait(d time.Duration) Option { return func(c *config) { c.defaultWait = d } }

// WithDefaultFairness sets the default fairness policy.
func WithDefaultFairness(f Fairness) Option { return func(c *config) { c.defaultFairness = f } }

// WithBlockingPoolSize caps dedicated connections used for blocking BLPOP waits (SPEC §15).
func WithBlockingPoolSize(n int) Option { return func(c *config) { c.blockingPoolSize = n } }

// WithRequireOwnerID forces an explicit Owner on every acquire (else it auto-defaults).
func WithRequireOwnerID(b bool) Option { return func(c *config) { c.requireOwnerID = b } }

// WithExtensionMargin sets the client-side safety margin for extend (SPEC §9.2).
func WithExtensionMargin(d time.Duration) Option { return func(c *config) { c.extensionMargin = d } }

// WithDeliveryFunctions forces the Redis FUNCTION (FCALL) delivery path.
func WithDeliveryFunctions() Option { return func(c *config) { c.delivery = deliveryFunctions } }

// WithDeliveryScripts forces the SCRIPT LOAD / EVALSHA delivery path.
func WithDeliveryScripts() Option { return func(c *config) { c.delivery = deliveryScripts } }

// WithAllowIncompatibleProtocol contends even if the server's protocol version differs.
func WithAllowIncompatibleProtocol(b bool) Option { return func(c *config) { c.allowIncompatibleVer = b } }

// WithKeyspaceEvents enables (default) or disables the auto-detected keyspace-expiry
// recovery subscriber. Never calls CONFIG SET; a no-op if the server lacks the events.
func WithKeyspaceEvents(b bool) Option { return func(c *config) { c.keyspaceEvents = b } }

// WithOnRecovery registers a callback invoked after a keyspace-expiry-triggered sweep.
func WithOnRecovery(fn func(resource string)) Option { return func(c *config) { c.onRecovery = fn } }

// WithLogger sets a structured logger (default: discard).
func WithLogger(lg *slog.Logger) Option {
	return func(c *config) {
		if lg != nil {
			c.logger = lg
		}
	}
}

// acquireOptions are resolved per acquire.
type acquireOptions struct {
	lease          time.Duration
	wait           time.Duration
	fairness       Fairness
	ownerID        string
	maxReaderBatch int
	watchdog       bool
	hasLease       bool
	hasWait        bool
	hasFairness    bool
	hasMaxBatch    bool
}

// AcquireOption configures a single acquire.
type AcquireOption func(*acquireOptions)

// Lease sets how long the lock may be held once granted.
func Lease(d time.Duration) AcquireOption {
	return func(o *acquireOptions) { o.lease = d; o.hasLease = true }
}

// Wait sets how long to block waiting for the lock.
func Wait(d time.Duration) AcquireOption {
	return func(o *acquireOptions) { o.wait = d; o.hasWait = true }
}

// Policy sets the fairness policy for this acquire.
func Policy(f Fairness) AcquireOption {
	return func(o *acquireOptions) { o.fairness = f; o.hasFairness = true }
}

// Owner sets the "who holds it" identity (defaults to <hostname>#<pid>).
func Owner(id string) AcquireOption { return func(o *acquireOptions) { o.ownerID = id } }

// MaxReaderBatch caps how many readers a single grant may wake.
func MaxReaderBatch(n int) AcquireOption {
	return func(o *acquireOptions) { o.maxReaderBatch = n; o.hasMaxBatch = true }
}

// Watchdog auto-extends the lease at ~lease/3 while held, and cancels the handle's
// Context if the lock is ever lost.
func Watchdog() AcquireOption { return func(o *acquireOptions) { o.watchdog = true } }
