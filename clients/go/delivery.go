package rwlock

import (
	"context"

	"github.com/mmdalix/redis-rwlock/clients/go/internal/lua"
	"github.com/redis/go-redis/v9"
)

// delivery runs a shared Lua operation by name, either via FCALL (Functions) or
// EVALSHA (scripts). Same source either way, so semantics are identical (SPEC §17).
type delivery interface {
	run(ctx context.Context, name string, keys []string, args []interface{}) (interface{}, error)
	mode() string
}

// functionDelivery calls the installed `rwlock` FUNCTION library via FCALL — routed by
// key on a Cluster client.
type functionDelivery struct{ client redis.UniversalClient }

func (d functionDelivery) mode() string { return "functions" }

func (d functionDelivery) run(ctx context.Context, name string, keys []string, args []interface{}) (interface{}, error) {
	return d.client.FCall(ctx, lua.FnName(name), keys, args...).Result()
}

// scriptDelivery uses SCRIPT LOAD / EVALSHA with an automatic EVAL fallback on NOSCRIPT
// (handled by go-redis's *redis.Script).
type scriptDelivery struct {
	client  redis.Scripter
	scripts map[string]*redis.Script
}

func newScriptDelivery(client redis.Scripter) scriptDelivery {
	scripts := make(map[string]*redis.Script, len(lua.Scripts))
	for name, src := range lua.Scripts {
		scripts[name] = redis.NewScript(src)
	}
	return scriptDelivery{client: client, scripts: scripts}
}

func (d scriptDelivery) mode() string { return "scripts" }

func (d scriptDelivery) run(ctx context.Context, name string, keys []string, args []interface{}) (interface{}, error) {
	return d.scripts[name].Run(ctx, d.client, keys, args...).Result()
}
