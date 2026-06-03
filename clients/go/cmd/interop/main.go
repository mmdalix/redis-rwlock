// Command interop is a tiny CLI used by the cross-language interop test: it acquires a
// lock (write/read) on a shared Redis, prints its fencing token, optionally holds, then
// releases. Paired with the Node interop script to prove Go and Node serialize on the
// same resource via the same shared Lua. Output protocol (one line):
//
//	RESULT FENCING <n>   (acquired)
//	RESULT TIMEOUT       (wait expired)
//	RESULT ERROR <msg>
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	rwlock "github.com/mmdalix/redis-rwlock/clients/go"
	"github.com/redis/go-redis/v9"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:6379", "redis address")
	mode := flag.String("mode", "write", "write|read")
	resource := flag.String("resource", "interop", "resource name")
	hold := flag.Duration("hold", 0, "how long to hold before releasing")
	wait := flag.Duration("wait", 5*time.Second, "acquire wait budget")
	flag.Parse()

	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: *addr})
	defer client.Close()
	l := rwlock.New(client)
	defer l.Close()

	opts := []rwlock.AcquireOption{rwlock.Wait(*wait), rwlock.Owner("go-interop")}
	var (
		h   *rwlock.Handle
		err error
	)
	if *mode == "read" {
		h, err = l.AcquireRead(ctx, *resource, opts...)
	} else {
		h, err = l.AcquireWrite(ctx, *resource, opts...)
	}
	switch {
	case errors.Is(err, rwlock.ErrWaitTimeout):
		fmt.Println("RESULT TIMEOUT")
		return
	case err != nil:
		fmt.Println("RESULT ERROR", err)
		os.Exit(1)
	}
	fmt.Printf("RESULT FENCING %d\n", h.FencingToken())
	_ = os.Stdout.Sync()
	if *hold > 0 {
		time.Sleep(*hold)
	}
	_ = h.Release(ctx)
}
