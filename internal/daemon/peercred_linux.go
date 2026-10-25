package daemon

import (
	"fmt"
	"net"
	"syscall"
)

// PeerUID is the uid of the process at the other end of c, from SO_PEERCRED.
// ok is false where the system does not tell.
func PeerUID(c net.Conn) (uid int, ok bool, err error) {
	uc, isUnix := c.(*net.UnixConn)
	if !isUnix {
		return 0, false, fmt.Errorf("not a unix socket connection")
	}
	raw, err := uc.SyscallConn()
	if err != nil {
		return 0, false, err
	}
	var cred *syscall.Ucred
	var credErr error
	if err := raw.Control(func(fd uintptr) {
		cred, credErr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); err != nil {
		return 0, false, err
	}
	if credErr != nil {
		return 0, false, credErr
	}
	return int(cred.Uid), true, nil
}

// checkPeer refuses a connection from a process of another user.
func checkPeer(c net.Conn) error {
	uid, _, err := PeerUID(c)
	if err != nil {
		return err
	}
	if uid != getuid() {
		return fmt.Errorf("peer uid %d is not uid %d", uid, getuid())
	}
	return nil
}
