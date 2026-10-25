//go:build unix

package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/CappyT/agent-courier/internal/p2p"
)

func TestRelayHelp(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := run([]string{"relay", "--help"}, &out, &errOut); code != 0 {
		t.Fatalf("relay --help = %d, stderr %q", code, errOut.String())
	}
	for _, want := range []string{"usage: agent-courier relay", "-key", "-max-reservations", "-max-circuits", "10m0s", "1MiB", "/udp/4001/quic-v1"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("relay --help lacks %q:\n%s", want, out.String())
		}
	}
}

// relayCtx stops a relay that starts when the test expects a refusal.
func relayCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestRelayBadArgs(t *testing.T) {
	key := filepath.Join(t.TempDir(), "relay.key")
	for _, args := range [][]string{
		{},
		{"--max-reservations", "2"},
		{"--key", key, "extra"},
		{"--key", key, "--max-data", "1MB"},
		{"--key", key, "--max-data", "0"},
		{"--key", key, "--max-duration", "soon"},
		{"--key", key, "--max-reservations", "x"},
		{"--bogus"},
	} {
		var out, errOut bytes.Buffer
		if code := serveRelay(relayCtx(t), args, &out, &errOut); code != 2 {
			t.Errorf("relay %q = %d, want 2; stderr %q", args, code, errOut.String())
		}
	}
}

func TestRelayGenKey(t *testing.T) {
	key := filepath.Join(t.TempDir(), "relay.key")
	var out, errOut bytes.Buffer
	if code := serveRelay(relayCtx(t), []string{"--key", key, "--gen-key"}, &out, &errOut); code != 0 {
		t.Fatalf("gen-key = %d, stderr %q", code, errOut.String())
	}
	if !strings.HasPrefix(out.String(), "12D3KooW") {
		t.Fatalf("gen-key printed %q, want a peer id", out.String())
	}
	fi, err := os.Stat(key)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("key mode %v, want 0600", fi.Mode().Perm())
	}
	if code := serveRelay(relayCtx(t), []string{"--key", key, "--gen-key"}, &out, &errOut); code != 1 {
		t.Fatalf("gen-key over an existing key = %d, want 1", code)
	}
}

func TestRelayKeyMode(t *testing.T) {
	key := filepath.Join(t.TempDir(), "relay.key")
	if _, err := p2p.GenerateRelayKey(key); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(key, 0o644); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	args := []string{"--key", key, "--listen", "/ip4/127.0.0.1/tcp/0"}
	if code := serveRelay(relayCtx(t), args, &out, &errOut); code != 1 {
		t.Fatalf("relay with a 0644 key = %d, want 1", code)
	}
	if !strings.Contains(errOut.String(), "readable by others or writable by the group") {
		t.Fatalf("stderr %q, want the mode refusal", errOut.String())
	}
}

// runCLIRelay runs the relay command until it logs a stats line, stops it,
// and returns the relay's peer id, its number of listen addresses and the logs.
func runCLIRelay(t *testing.T, args []string) (id string, addrs int, logs string) {
	t.Helper()
	started := make(chan *p2p.Relay, 1)
	relayStarted = func(r *p2p.Relay) { started <- r }
	statsInterval = 10 * time.Millisecond
	t.Cleanup(func() {
		relayStarted = func(*p2p.Relay) {}
		statsInterval = time.Minute
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var out syncBuffer
	code := make(chan int, 1)
	go func() { code <- serveRelay(ctx, args, &bytes.Buffer{}, &out) }()

	var r *p2p.Relay
	select {
	case r = <-started:
	case c := <-code:
		t.Fatalf("relay exited with %d: %s", c, out.String())
	case <-time.After(10 * time.Second):
		t.Fatal("the relay did not start")
	}
	id, addrs = r.ID().String(), len(r.Addrs())
	deadline := time.Now().Add(10 * time.Second)
	for !strings.Contains(out.String(), "relay stats") && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case c := <-code:
		if c != 0 {
			t.Fatalf("relay exited with %d: %s", c, out.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the relay did not stop")
	}
	return id, addrs, out.String()
}

func TestRelayCommand(t *testing.T) {
	key := filepath.Join(t.TempDir(), "relay.key")
	id, err := p2p.GenerateRelayKey(key)
	if err != nil {
		t.Fatal(err)
	}
	got, addrs, out := runCLIRelay(t, []string{
		"--key", key,
		"--listen", "/ip4/127.0.0.1/tcp/0,/ip4/127.0.0.1/udp/0/quic-v1",
		"--announce", "/ip4/1.2.3.4/tcp/4001", "--announce", "/dns4/relay.example.com/udp/4001/quic-v1",
		"--max-reservations", "30", "--max-reservations-per-ip", "9", "--max-circuits", "5",
		"--reservation-ttl", "2m", "--max-duration", "30s", "--max-data", "64KiB",
	})
	if got != id.String() {
		t.Fatalf("relay id %s, want the id of the key file %s", got, id)
	}
	if addrs != 2 {
		t.Fatalf("relay has %d listen addresses, want one TCP and one QUIC", addrs)
	}
	for _, want := range []string{
		"relay started", "max_reservations=30", "max_reservations_per_ip=9", "max_circuits=5",
		"reservation_ttl=2m0s", "max_duration=30s", "max_data=65536",
		`advertised="[/ip4/1.2.3.4/tcp/4001 /dns4/relay.example.com/udp/4001/quic-v1]"`,
		"relay stats", "reservations=0", "reservations_refused=none", "circuits_refused=none", "relay stopped",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("logs lack %q:\n%s", want, out)
		}
	}
	for _, bad := range []string{id.String(), "no advertised address is public"} {
		if strings.Contains(out, bad) {
			t.Errorf("logs hold %q:\n%s", bad, out)
		}
	}
}

func TestRelayCommandWarnsWithoutPublicAddr(t *testing.T) {
	key := filepath.Join(t.TempDir(), "relay.key")
	if _, err := p2p.GenerateRelayKey(key); err != nil {
		t.Fatal(err)
	}
	_, _, out := runCLIRelay(t, []string{"--key", key, "--listen", "/ip4/127.0.0.1/tcp/0"})
	if !strings.Contains(out, "level=WARN") || !strings.Contains(out, "no advertised address is public") {
		t.Fatalf("logs lack the warning:\n%s", out)
	}
}

func TestRelayAnnounceRefused(t *testing.T) {
	key := filepath.Join(t.TempDir(), "relay.key")
	if _, err := p2p.GenerateRelayKey(key); err != nil {
		t.Fatal(err)
	}
	for addr, rule := range map[string]string{
		"/ip4/192.168.1.10/tcp/4001":        "must be a public address",
		"/ip4/1.2.3.4/tcp/4001/p2p-circuit": "must not carry /p2p or /p2p-circuit",
	} {
		var out, errOut bytes.Buffer
		args := []string{"--key", key, "--listen", "/ip4/127.0.0.1/tcp/0", "--announce", addr}
		if code := serveRelay(relayCtx(t), args, &out, &errOut); code != 1 {
			t.Errorf("announce %s = %d, want 1", addr, code)
		}
		if !strings.Contains(errOut.String(), rule) {
			t.Errorf("announce %s: stderr %q, want %q", addr, errOut.String(), rule)
		}
	}
}

func TestRelayPrintID(t *testing.T) {
	key := filepath.Join(t.TempDir(), "relay.key")
	id, err := p2p.GenerateRelayKey(key)
	if err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	if code := serveRelay(relayCtx(t), []string{"--key", key, "--print-id"}, &out, &errOut); code != 0 {
		t.Fatalf("print-id = %d, stderr %q", code, errOut.String())
	}
	if got := strings.TrimSpace(out.String()); got != id.String() {
		t.Fatalf("print-id printed %q, want %s", got, id)
	}
	if err := os.Chmod(key, 0o644); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if code := serveRelay(relayCtx(t), []string{"--key", key, "--print-id"}, &out, &errOut); code != 1 || out.Len() != 0 {
		t.Fatalf("print-id of a 0644 key = %d, stdout %q, want 1 and nothing", code, out.String())
	}
}

func TestFormatCounts(t *testing.T) {
	if got := formatCounts(map[string]int64{"NO_RESERVATION": 2, "PERMISSION_DENIED": 1}); got != "NO_RESERVATION=2,PERMISSION_DENIED=1" {
		t.Fatalf("formatCounts = %q", got)
	}
}

func TestParseSize(t *testing.T) {
	for in, want := range map[string]int64{"1MiB": 1 << 20, "16KiB": 16 << 10, "2GiB": 2 << 30, "512": 512, "100B": 100} {
		if got, err := parseSize(in); err != nil || got != want {
			t.Errorf("parseSize(%q) = %d, %v, want %d", in, got, err, want)
		}
		if back, err := parseSize(formatSize(want)); err != nil || back != want {
			t.Errorf("parseSize(formatSize(%d)) = %d, %v", want, back, err)
		}
	}
	for _, in := range []string{"", "0", "-1", "1MB", "MiB", "9999999999GiB"} {
		if _, err := parseSize(in); err == nil {
			t.Errorf("parseSize(%q) succeeded", in)
		}
	}
}
