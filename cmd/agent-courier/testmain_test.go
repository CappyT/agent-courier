//go:build unix

package main

import (
	"os"
	"testing"
)

// TestMain keeps the p2p hosts of the daemons in the tests on loopback and
// away from the project relay.
func TestMain(m *testing.M) {
	p2pListen = func() []string { return []string{"/ip4/127.0.0.1/tcp/0", "/ip4/127.0.0.1/udp/0/quic-v1"} }
	p2pRelays = func() []string { return nil }
	os.Exit(m.Run())
}
