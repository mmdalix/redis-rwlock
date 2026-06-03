package rwlock_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	rwlock "github.com/mmdalix/redis-rwlock/clients/go"
	"github.com/mmdalix/redis-rwlock/clients/go/internal/redistest"
	"github.com/redis/go-redis/v9"
)

// Runs the shared, language-agnostic conformance scenarios (protocol/conformance)
// against the Go client — the same files the Node client must pass (SPEC §20.10).

type scenarioStep struct {
	Op       string `json:"op"`
	As       string `json:"as"`
	Handle   string `json:"handle"`
	Than     string `json:"than"`
	Resource string `json:"resource"`
	Mode     string `json:"mode"`
	LeaseMs  int    `json:"leaseMs"`
	WaitMs   int    `json:"waitMs"`
	Expect   string `json:"expect"`
}

type scenario struct {
	Name  string         `json:"name"`
	Steps []scenarioStep `json:"steps"`
}

func TestConformanceScenarios(t *testing.T) {
	dir := filepath.Join("..", "..", "protocol", "conformance", "scenarios")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read scenarios: %v", err)
	}
	addr := redistest.Start(t).Options().Addr

	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".json" {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", e.Name(), err)
		}
		var sc scenario
		if err := json.Unmarshal(raw, &sc); err != nil {
			t.Fatalf("parse %s: %v", e.Name(), err)
		}
		t.Run(sc.Name, func(t *testing.T) {
			runScenario(t, addr, sc)
		})
	}
}

func runScenario(t *testing.T, addr string, sc scenario) {
	ctx := context.Background()
	l := rwlock.New(redis.NewClient(&redis.Options{Addr: addr}))
	t.Cleanup(func() { _ = l.Close() })

	handles := map[string]*rwlock.Handle{}
	ns := 0
	res := func(r string) string { return fmt.Sprintf("conf:%s:%s", sc.Name, r) }

	for i, step := range sc.Steps {
		switch step.Op {
		case "acquire":
			opts := []rwlock.AcquireOption{rwlock.Owner(fmt.Sprintf("c%d", ns))}
			ns++
			if step.LeaseMs > 0 {
				opts = append(opts, rwlock.Lease(time.Duration(step.LeaseMs)*time.Millisecond))
			}
			if step.WaitMs > 0 {
				opts = append(opts, rwlock.Wait(time.Duration(step.WaitMs)*time.Millisecond))
			}
			var h *rwlock.Handle
			var err error
			if step.Mode == "read" {
				h, err = l.AcquireRead(ctx, res(step.Resource), opts...)
			} else {
				h, err = l.AcquireWrite(ctx, res(step.Resource), opts...)
			}
			if step.Expect == "timeout" {
				if !errors.Is(err, rwlock.ErrWaitTimeout) {
					t.Fatalf("step %d (%s): err=%v, want ErrWaitTimeout", i, sc.Name, err)
				}
			} else {
				if err != nil {
					t.Fatalf("step %d (%s): acquire: %v", i, sc.Name, err)
				}
				if step.As != "" {
					handles[step.As] = h
				}
			}
		case "release":
			h := handles[step.Handle]
			if h == nil {
				t.Fatalf("step %d (%s): handle %q not found", i, sc.Name, step.Handle)
			}
			if err := h.Release(ctx); err != nil {
				t.Fatalf("step %d (%s): release: %v", i, sc.Name, err)
			}
		case "expectFencingGt":
			a, b := handles[step.Handle], handles[step.Than]
			if a == nil || b == nil {
				t.Fatalf("step %d (%s): handles for fencing compare not found", i, sc.Name)
			}
			if !(a.FencingToken() > b.FencingToken()) {
				t.Fatalf("step %d (%s): fencing %d not > %d", i, sc.Name, a.FencingToken(), b.FencingToken())
			}
		default:
			t.Fatalf("step %d (%s): unknown op %q", i, sc.Name, step.Op)
		}
	}
}
