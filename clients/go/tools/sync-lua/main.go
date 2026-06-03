// Command sync-lua vendors the canonical Lua (protocol/lua) into the Go module's
// internal/lua directory, so it can be //go:embed'ed (Go can't embed outside the
// module). Run from clients/go:  go run ./tools/sync-lua
//
// internal/lua is GENERATED — never hand-edit it; edit protocol/lua and re-run this.
// CI also runs the check-sync test (TestVendoredLuaInSync) to fail on drift.
package main

import (
	"fmt"
	"os"
	"path/filepath"
)

var files = []string{"lib", "acquire", "release", "extend", "cancel_wait", "expire_and_grant", "inspect"}

func main() {
	const (
		src = "../../protocol/lua"
		dst = "internal/lua"
	)
	for _, name := range files {
		b, err := os.ReadFile(filepath.Join(src, name+".lua"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "sync-lua: read %s: %v\n", name, err)
			os.Exit(1)
		}
		if err := os.WriteFile(filepath.Join(dst, name+".lua"), b, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "sync-lua: write %s: %v\n", name, err)
			os.Exit(1)
		}
	}
	fmt.Printf("synced %d Lua files from %s to %s\n", len(files), src, dst)
}
