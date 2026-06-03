package rwlock

import (
	"context"
	"regexp"
	"strings"
	"time"
)

var resourceRe = regexp.MustCompile(`^rwlock:\{(.+)\}:`)

func resourceFromKey(key string) (string, bool) {
	m := resourceRe.FindStringSubmatch(key)
	if m == nil {
		return "", false
	}
	return m[1], true
}

// detectKeyspaceEvents reports whether the server emits expired keyevents (needs the
// keyevent flag E plus x, or A which includes x). Best-effort; CONFIG denied -> false.
func (l *RwLock) detectKeyspaceEvents(ctx context.Context) bool {
	m, err := l.client.ConfigGet(ctx, "notify-keyspace-events").Result()
	if err != nil {
		return false
	}
	flags := m["notify-keyspace-events"]
	return strings.Contains(flags, "E") && (strings.Contains(flags, "x") || strings.Contains(flags, "A"))
}

// startKeyspace runs a subscriber that, on any rwlock:{r}:* key expiry (notably the
// writer key's native TTL), runs expire_and_grant(r) — promptly granting waiters
// out-of-band. We never enable keyspace events ourselves (no CONFIG SET).
func (l *RwLock) startKeyspace() {
	ctx, cancel := context.WithCancel(context.Background())
	ps := l.client.PSubscribe(ctx, "__keyevent@*__:expired")
	l.ksCancel = cancel
	l.ksPubSub = ps

	ch := ps.Channel()
	go func() {
		for msg := range ch {
			resource, ok := resourceFromKey(msg.Payload)
			if !ok {
				continue
			}
			cctx, c := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := l.delivery.run(cctx, "expire_and_grant", []string{keyPrefix(resource)}, []interface{}{msOf(l.cfg.notifyKeyTTL)})
			c()
			if err != nil {
				l.cfg.logger.Debug("rwlock: keyspace expire_and_grant failed", "resource", resource, "err", err)
				continue
			}
			if l.cfg.onRecovery != nil {
				l.cfg.onRecovery(resource)
			}
		}
	}()
}

func (l *RwLock) stopKeyspace() {
	if l.ksCancel != nil {
		l.ksCancel()
		l.ksCancel = nil
	}
	if l.ksPubSub != nil {
		_ = l.ksPubSub.Close()
		l.ksPubSub = nil
	}
}

