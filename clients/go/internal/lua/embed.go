// Package lua embeds the canonical Redis Lua (vendored from protocol/lua by
// tools/sync-lua) and builds the two delivery shapes from the same source:
//
//   - Scripts[name]    = lib.lua + body, for SCRIPT LOAD / EVALSHA.
//   - FunctionLibrary  = a single Redis FUNCTION library (#!lua name=rwlock) that
//                        wraps each body in redis.register_function.
//
// The .lua files here are GENERATED — edit protocol/lua and re-run the sync step.
package lua

import (
	"embed"
	"strings"
)

// ProtocolVersion mirrors protocol/VERSION; tools/sync-lua keeps them in lockstep.
const ProtocolVersion = 1

//go:embed *.lua
var files embed.FS

// scriptNames is the fixed set + order of the per-resource scripts.
var scriptNames = []string{"acquire", "release", "extend", "cancel_wait", "expire_and_grant", "inspect"}

// FnName maps a script name to its registered FUNCTION name (FCALL target).
func FnName(script string) string { return "rwlock_" + script }

var (
	// Scripts maps a script name to its standalone EVALSHA body (lib + script).
	Scripts = map[string]string{}
	// FunctionLibrary is the loadable `rwlock` FUNCTION library source.
	FunctionLibrary string
)

func mustRead(name string) string {
	b, err := files.ReadFile(name)
	if err != nil {
		panic("rwlock/internal/lua: missing embedded " + name + ": " + err.Error())
	}
	return string(b)
}

func init() {
	lib := mustRead("lib.lua")

	var fn strings.Builder
	fn.WriteString("#!lua name=rwlock\n")
	fn.WriteString(lib)
	fn.WriteString("\n")

	for _, name := range scriptNames {
		body := mustRead(name + ".lua")
		Scripts[name] = lib + "\n" + body

		callback := "function(keys, args)\n  local KEYS, ARGV = keys, args\n" + body + "\nend"
		if name == "inspect" {
			// read-only -> register with the no-writes flag (runnable on replicas).
			fn.WriteString("redis.register_function{ function_name = 'rwlock_inspect', flags = { 'no-writes' }, callback = " + callback + " }\n\n")
		} else {
			fn.WriteString("redis.register_function('" + FnName(name) + "', " + callback + ")\n\n")
		}
	}
	FunctionLibrary = fn.String()
}
