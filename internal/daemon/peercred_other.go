//go:build unix && !linux

package daemon

import "net"

// PeerUID does not tell the peer's uid on this system: the 0700 socket
// directory already keeps out the processes of other users.
func PeerUID(net.Conn) (uid int, ok bool, err error) { return 0, false, nil }

// checkPeer accepts every connection, see PeerUID.
func checkPeer(net.Conn) error { return nil }
