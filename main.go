package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	pkgsrc "github.com/guilherme13c/tinyKV/src"
	"github.com/guilherme13c/tinyKV/src/store"
)

func main() {
	dir := flag.String("dir", "data", "directory for SSTable files and the manifest")
	flag.Parse()

	if err := os.MkdirAll(*dir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir %q: %v\n", *dir, err)
		os.Exit(1)
	}

	walPath := filepath.Join(*dir, "wal")
	s, err := store.NewStore(walPath, *dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open store: %v\n", err)
		os.Exit(1)
	}

	// Graceful shutdown on SIGINT / SIGTERM.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		fmt.Println("\nshutting down…")
		if err := s.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close: %v\n", err)
		}
		os.Exit(0)
	}()

	fmt.Println("tinyKV — commands: put <key> <value> | get <key> | delete <key> | scan <start> <end> | exit")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break // EOF (Ctrl+D)
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, " ", 3)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "exit", "quit":
			goto shutdown

		case "put":
			if len(parts) < 3 {
				fmt.Fprintln(os.Stderr, "usage: put <key> <value>")
				continue
			}
			if err := s.Put([]byte(parts[1]), []byte(parts[2])); err != nil {
				fmt.Fprintf(os.Stderr, "put: %v\n", err)
				continue
			}
			fmt.Println("ok")

		case "get":
			if len(parts) < 2 {
				fmt.Fprintln(os.Stderr, "usage: get <key>")
				continue
			}
			val, err := s.Get([]byte(parts[1]))
			if err != nil {
				if errors.Is(err, pkgsrc.ErrKeyNotFound) {
					fmt.Println("(not found)")
				} else {
					fmt.Fprintf(os.Stderr, "get: %v\n", err)
				}
				continue
			}
			fmt.Println(string(val))

		case "delete":
			if len(parts) < 2 {
				fmt.Fprintln(os.Stderr, "usage: delete <key>")
				continue
			}
			if err := s.Delete([]byte(parts[1])); err != nil {
				fmt.Fprintf(os.Stderr, "delete: %v\n", err)
				continue
			}
			fmt.Println("ok")

		case "scan":
			if len(parts) < 3 {
				fmt.Fprintln(os.Stderr, "usage: scan <startKey> <endKey>")
				continue
			}
			it, err := s.Scan([]byte(parts[1]), []byte(parts[2]))
			if err != nil {
				fmt.Fprintf(os.Stderr, "scan: %v\n", err)
				continue
			}
			count := 0
			for ; it.Valid(); it.Next() {
				fmt.Printf("  %s = %s\n", it.Key(), it.Value())
				count++
			}
			_ = it.Close()
			if count == 0 {
				fmt.Println("(no results)")
			}

		default:
			fmt.Fprintf(os.Stderr, "unknown command %q\n", cmd)
		}
	}

shutdown:
	if err := s.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "close: %v\n", err)
		os.Exit(1)
	}
}
