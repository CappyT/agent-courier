//go:build unix

package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"

	"github.com/CappyT/agent-courier/internal/client"
	"github.com/CappyT/agent-courier/internal/daemon"
)

// runBridge connects to the daemon with the client's checks and autostart,
// then copies lines between stdin and stdout and the socket. The OpenCode
// plugin speaks the wire protocol through it. design.md "OpenCode client".
func runBridge(stdin io.Reader, stdout, stderr io.Writer) int {
	log := slog.New(slog.NewTextHandler(stderr, nil))
	conn, err := client.Dial(context.Background())
	if err != nil {
		log.Error("connect to the courier daemon", "err", err)
		return 1
	}
	return bridge(conn, stdin, stdout, log)
}

var (
	errLineCap    = fmt.Errorf("a line has more than %d bytes", daemon.MaxLine)
	errDaemonGone = errors.New("the courier daemon closed the connection")
)

// bridge copies lines in both directions until either side closes or sends a
// line over the cap. Closed stdin is the normal end.
func bridge(conn net.Conn, stdin io.Reader, stdout io.Writer, log *slog.Logger) int {
	defer conn.Close()
	ends := make(chan error, 2)
	go func() {
		err := copyLines(conn, stdin)
		if err != nil {
			err = fmt.Errorf("from stdin: %w", err)
		}
		ends <- err
	}()
	go func() {
		err := copyLines(stdout, conn)
		if err == nil {
			err = errDaemonGone
		} else {
			err = fmt.Errorf("from the daemon: %w", err)
		}
		ends <- err
	}()
	if err := <-ends; err != nil {
		log.Error("bridge stopped", "err", err)
		return 1
	}
	return 0
}

// copyLines writes each line of src to dst with one write. It returns nil at
// the end of src and drops a last line without a newline. A line over
// daemon.MaxLine without its newline stops the copy before it is written.
func copyLines(dst io.Writer, src io.Reader) error {
	// Room for a line of MaxLine bytes and its \r\n.
	r := bufio.NewReaderSize(src, daemon.MaxLine+2)
	for {
		line, err := r.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			return errLineCap
		}
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		if len(bytes.TrimSuffix(bytes.TrimSuffix(line, []byte("\n")), []byte("\r"))) > daemon.MaxLine {
			return errLineCap
		}
		if _, err := dst.Write(line); err != nil {
			return err
		}
	}
}
