// Package e2e runs end-to-end tests against the compiled tinyKV binary.
// TestMain builds the binary once; each test drives it through stdin and
// validates stdout responses.
package e2e_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

var binaryPath string

func TestMain(m *testing.M) {
	tmp, err := os.MkdirTemp("", "tinykv-bin-*")
	if err != nil {
		panic("MkdirTemp: " + err.Error())
	}
	defer os.RemoveAll(tmp)

	binaryPath = filepath.Join(tmp, "tinykv")
	out, err := exec.Command("go", "build", "-o", binaryPath, "github.com/guilherme13c/tinyKV").CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "build failed: %v\n%s\n", err, out)
		os.Exit(1)
	}

	os.Exit(m.Run())
}

// run executes the binary with the given data directory and commands.
// "exit" is appended automatically. Returns one response string per command
// (scan entries are returned as individual "key = value" strings).
func run(t *testing.T, dir string, commands ...string) []string {
	t.Helper()
	input := strings.Join(append(commands, "exit"), "\n") + "\n"
	cmd := exec.Command(binaryPath, "-dir", dir)
	cmd.Stdin = strings.NewReader(input)
	out, err := cmd.Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			t.Fatalf("binary error: %v\nstderr: %s\nstdout: %s", err, ee.Stderr, out)
		}
		t.Fatalf("run: %v", err)
	}
	return parseResponses(string(out))
}

// parseResponses extracts response content from the binary's stdout.
//
// Output format:
//
//	tinyKV — commands: …          <- header (skipped)
//	> ok                           <- prompt + response on same line
//	>   key = val                  <- prompt + first scan entry
//	  key2 = val2                  <- subsequent scan entries (indented)
//	>                              <- bare prompt before exit (skipped)
func parseResponses(output string) []string {
	var result []string
	lines := strings.Split(strings.TrimRight(output, "\n"), "\n")
	if len(lines) == 0 {
		return nil
	}
	for _, line := range lines[1:] { // skip header
		switch {
		case strings.HasPrefix(line, "> "):
			if content := strings.TrimSpace(strings.TrimPrefix(line, "> ")); content != "" {
				result = append(result, content)
			}
		case strings.HasPrefix(line, "  "): // scan continuation lines
			if content := strings.TrimSpace(line); content != "" {
				result = append(result, content)
			}
		}
	}
	return result
}

// ── Basic operations ────────────────────────────────────────────────────────

func TestE2EPutGet(t *testing.T) {
	resp := run(t, t.TempDir(), "put hello world", "get hello")
	if len(resp) != 2 {
		t.Fatalf("want 2 responses, got %d: %v", len(resp), resp)
	}
	if resp[0] != "ok" {
		t.Errorf("put: got %q, want %q", resp[0], "ok")
	}
	if resp[1] != "world" {
		t.Errorf("get: got %q, want %q", resp[1], "world")
	}
}

func TestE2EGetMissing(t *testing.T) {
	resp := run(t, t.TempDir(), "get no-such-key")
	if len(resp) != 1 || resp[0] != "(not found)" {
		t.Errorf("get missing: got %v, want [(not found)]", resp)
	}
}

func TestE2EOverwrite(t *testing.T) {
	resp := run(t, t.TempDir(), "put k v1", "put k v2", "get k")
	if len(resp) != 3 {
		t.Fatalf("want 3 responses, got %d: %v", len(resp), resp)
	}
	if resp[2] != "v2" {
		t.Errorf("get after overwrite: got %q, want %q", resp[2], "v2")
	}
}

func TestE2EDelete(t *testing.T) {
	resp := run(t, t.TempDir(), "put gone bye", "delete gone", "get gone")
	if len(resp) != 3 {
		t.Fatalf("want 3 responses, got %d: %v", len(resp), resp)
	}
	if resp[2] != "(not found)" {
		t.Errorf("get after delete: got %q, want %q", resp[2], "(not found)")
	}
}

func TestE2EDeleteNonExistent(t *testing.T) {
	// Deleting a key that was never written should still succeed (tombstone write).
	resp := run(t, t.TempDir(), "delete never-existed")
	if len(resp) != 1 || resp[0] != "ok" {
		t.Errorf("delete non-existent: got %v, want [ok]", resp)
	}
}

// ── Scan ────────────────────────────────────────────────────────────────────

func TestE2EScan(t *testing.T) {
	// Insert keys out of order; scan must return them sorted.
	resp := run(t, t.TempDir(),
		"put b 2",
		"put a 1",
		"put c 3",
		"scan a d",
	)
	// 3 x "ok" + 3 scan entries
	if len(resp) != 6 {
		t.Fatalf("want 6 responses, got %d: %v", len(resp), resp)
	}
	want := []string{"a = 1", "b = 2", "c = 3"}
	for i, w := range want {
		if resp[3+i] != w {
			t.Errorf("scan[%d]: got %q, want %q", i, resp[3+i], w)
		}
	}
}

func TestE2EScanEndKeyExclusive(t *testing.T) {
	resp := run(t, t.TempDir(),
		"put a 1", "put b 2", "put c 3",
		"scan a c", // end key is exclusive — c must be absent
	)
	// 3 x "ok" + 2 scan entries (a, b)
	if len(resp) != 5 {
		t.Fatalf("want 5 responses, got %d: %v", len(resp), resp)
	}
	if resp[3] != "a = 1" || resp[4] != "b = 2" {
		t.Errorf("scan with exclusive end: got %v, want [a = 1, b = 2]", resp[3:])
	}
}

func TestE2EScanEmpty(t *testing.T) {
	resp := run(t, t.TempDir(), "scan aaa zzz")
	if len(resp) != 1 || resp[0] != "(no results)" {
		t.Errorf("scan empty store: got %v, want [(no results)]", resp)
	}
}

func TestE2EScanTombstonesExcluded(t *testing.T) {
	resp := run(t, t.TempDir(),
		"put a 1", "put b 2", "put c 3",
		"delete b",
		"scan a d",
	)
	// 3 puts + 1 delete = 4 x "ok", then 2 scan entries (a, c)
	if len(resp) != 6 {
		t.Fatalf("want 6 responses, got %d: %v", len(resp), resp)
	}
	if resp[4] != "a = 1" || resp[5] != "c = 3" {
		t.Errorf("scan after delete: got %v, want [a = 1, c = 3]", resp[4:])
	}
}

// ── Persistence ─────────────────────────────────────────────────────────────

func TestE2EPersistence(t *testing.T) {
	dir := t.TempDir()

	run(t, dir, "put persist-key persist-val")

	resp := run(t, dir, "get persist-key")
	if len(resp) != 1 || resp[0] != "persist-val" {
		t.Errorf("get after reopen: got %v, want [persist-val]", resp)
	}
}

func TestE2EPersistenceAfterDelete(t *testing.T) {
	dir := t.TempDir()

	run(t, dir, "put k v", "delete k")

	resp := run(t, dir, "get k")
	if len(resp) != 1 || resp[0] != "(not found)" {
		t.Errorf("get after reopen+delete: got %v, want [(not found)]", resp)
	}
}

func TestE2EPersistenceMultipleKeys(t *testing.T) {
	dir := t.TempDir()
	const n = 10

	// Write n keys.
	puts := make([]string, n)
	for i := range n {
		puts[i] = fmt.Sprintf("put key-%02d val-%02d", i, i)
	}
	run(t, dir, puts...)

	// Read them back in a fresh process.
	gets := make([]string, n)
	for i := range n {
		gets[i] = fmt.Sprintf("get key-%02d", i)
	}
	resp := run(t, dir, gets...)
	if len(resp) != n {
		t.Fatalf("want %d responses, got %d: %v", n, len(resp), resp)
	}
	for i, r := range resp {
		want := fmt.Sprintf("val-%02d", i)
		if r != want {
			t.Errorf("get key-%02d: got %q, want %q", i, r, want)
		}
	}
}

// ── Mixed workload ───────────────────────────────────────────────────────────

func TestE2EOverwriteThenScan(t *testing.T) {
	resp := run(t, t.TempDir(),
		"put x old",
		"put x new",
		"put y y-val",
		"scan x z",
	)
	// 3 x "ok" + 2 scan entries (x, y)
	if len(resp) != 5 {
		t.Fatalf("want 5 responses, got %d: %v", len(resp), resp)
	}
	if resp[3] != "x = new" {
		t.Errorf("scan after overwrite: x: got %q, want %q", resp[3], "x = new")
	}
	if resp[4] != "y = y-val" {
		t.Errorf("scan after overwrite: y: got %q, want %q", resp[4], "y = y-val")
	}
}

func TestE2ELargeWorkload(t *testing.T) {
	dir := t.TempDir()
	const n = 50

	puts := make([]string, n)
	for i := range n {
		puts[i] = fmt.Sprintf("put key-%03d val-%03d", i, i)
	}
	run(t, dir, puts...)

	gets := make([]string, n)
	for i := range n {
		gets[i] = fmt.Sprintf("get key-%03d", i)
	}
	resp := run(t, dir, gets...)
	if len(resp) != n {
		t.Fatalf("want %d get responses, got %d", n, len(resp))
	}
	for i, r := range resp {
		want := fmt.Sprintf("val-%03d", i)
		if r != want {
			t.Errorf("get key-%03d: got %q, want %q", i, r, want)
		}
	}
}
