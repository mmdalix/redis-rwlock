// Package redistest spawns a throwaway redis-server per test (no Docker needed),
// mirroring the Node client's harness.
package redistest

import (
	"context"
	"net"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// Start launches a redis-server on a random port and returns a connected client.
// Extra args (e.g. "--notify-keyspace-events", "Ex") are appended. The server and
// client are torn down via t.Cleanup.
func Start(t *testing.T, extraArgs ...string) *redis.Client {
	t.Helper()
	port := freePort(t)
	dir := t.TempDir()
	args := append([]string{
		"--port", strconv.Itoa(port), "--save", "", "--appendonly", "no", "--dir", dir,
	}, extraArgs...)
	cmd := exec.Command("redis-server", args...)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start redis-server (is it on PATH?): %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})

	addr := "127.0.0.1:" + strconv.Itoa(port)
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		if err := client.Ping(ctx).Err(); err == nil {
			return client
		}
		select {
		case <-ctx.Done():
			t.Fatalf("redis-server on %s did not become ready", addr)
		case <-time.After(50 * time.Millisecond):
		}
	}
}
