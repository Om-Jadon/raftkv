package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/Om-Jadon/raftkv/internal/kv"
)

func main() {
	addrs := flag.String("addrs", "http://127.0.0.1:8001", "comma-separated HTTP addresses")
	flag.Parse()
	args := flag.Args()
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "usage: raftkvctl [-addrs url,...] get|put|delete|append KEY [VALUE]\n")
		os.Exit(2)
	}

	c := kv.NewClient(splitAddrs(*addrs))
	ctx := context.Background()
	var err error
	switch args[0] {
	case "get":
		if len(args) != 2 {
			fatal("get KEY")
		}
		var v string
		v, err = c.Get(ctx, args[1])
		if err == nil {
			fmt.Println(v)
		}
	case "put":
		if len(args) != 3 {
			fatal("put KEY VALUE")
		}
		err = c.Put(ctx, args[1], args[2])
	case "delete":
		if len(args) != 2 {
			fatal("delete KEY")
		}
		err = c.Delete(ctx, args[1])
	case "append":
		if len(args) != 3 {
			fatal("append KEY VALUE")
		}
		var v string
		v, err = c.Append(ctx, args[1], args[2])
		if err == nil {
			fmt.Println(v)
		}
	default:
		fatal("unknown command " + args[0])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func splitAddrs(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(2)
}
