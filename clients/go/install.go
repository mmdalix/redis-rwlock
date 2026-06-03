package rwlock

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/mmdalix/redis-rwlock/clients/go/internal/lua"
	"github.com/redis/go-redis/v9"
)

const (
	moduleKey   = "rwlock:__module__"
	implVersion = "0.1.0"
)

// moduleSHA is the SHA-1 of the canonical FUNCTION library — same protocol bytes ⇒
// same behavior across clients.
var moduleSHA = func() string {
	sum := sha1.Sum([]byte(lua.FunctionLibrary))
	return hex.EncodeToString(sum[:])
}()

// installAndHandshake performs the cross-version handshake (SPEC §16) and picks a
// delivery (Functions if available, else EVALSHA). loadedAtMs is Redis server time.
func installAndHandshake(ctx context.Context, client redis.UniversalClient, cfg config, loadedAtMs int64) (delivery, error) {
	marker, err := client.HGetAll(ctx, moduleKey).Result()
	if err != nil {
		return nil, fmt.Errorf("%w: read module marker: %v", ErrBackendUnavailable, err)
	}
	if v, ok := marker["protocol_version"]; ok && v != strconv.Itoa(lua.ProtocolVersion) && !cfg.allowIncompatibleVer {
		return nil, fmt.Errorf("%w: server speaks v%s, client v%d", ErrIncompatibleServerLogic, v, lua.ProtocolVersion)
	}

	var d delivery
	if cfg.delivery != deliveryScripts {
		if _, ferr := client.FunctionLoadReplace(ctx, lua.FunctionLibrary).Result(); ferr == nil {
			d = functionDelivery{client: client}
		} else if cfg.delivery == deliveryFunctions {
			return nil, fmt.Errorf("%w: FUNCTION LOAD: %v", ErrUnsupported, ferr)
		}
	}
	if d == nil {
		d = newScriptDelivery(client)
	}

	if len(marker) == 0 {
		// idempotent and safe under concurrent installers (identical values).
		if err := client.HSet(ctx, moduleKey,
			"protocol_version", lua.ProtocolVersion,
			"impl_version", implVersion,
			"sha", moduleSHA,
			"loaded_at_ms", loadedAtMs,
		).Err(); err != nil {
			return nil, fmt.Errorf("%w: write module marker: %v", ErrBackendUnavailable, err)
		}
	}
	return d, nil
}
