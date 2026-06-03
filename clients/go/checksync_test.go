package rwlock_test

import (
	"os"
	"path/filepath"
	"testing"
)

// TestVendoredLuaInSync guards against drift: clients/go/internal/lua must be a byte
// -for-byte copy of the canonical protocol/lua. If this fails, run `go run ./tools/sync-lua`.
func TestVendoredLuaInSync(t *testing.T) {
	files := []string{"lib", "acquire", "release", "extend", "cancel_wait", "expire_and_grant", "inspect"}
	for _, name := range files {
		canonical, err := os.ReadFile(filepath.Join("..", "..", "protocol", "lua", name+".lua"))
		if err != nil {
			t.Fatalf("read canonical %s: %v", name, err)
		}
		vendored, err := os.ReadFile(filepath.Join("internal", "lua", name+".lua"))
		if err != nil {
			t.Fatalf("read vendored %s: %v", name, err)
		}
		if string(canonical) != string(vendored) {
			t.Fatalf("internal/lua/%s.lua drifted from protocol/lua/%s.lua — run `go run ./tools/sync-lua`", name, name)
		}
	}
}
