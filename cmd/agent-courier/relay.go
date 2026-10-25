//go:build unix

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/CappyT/agent-courier/internal/p2p"
)

const relayUsage = `usage: agent-courier relay --key <file> [flags]

Run a libp2p circuit relay v2 for team members whose daemons cannot connect
directly. The relay keeps no messages, and logs only its own addresses and
counts.

flags:
`

// relayStarted and statsInterval are variables so tests can reach the
// running relay and see a stats line without a long wait.
var (
	relayStarted  = func(*p2p.Relay) {}
	statsInterval = time.Minute
)

func runRelay(args []string, stdout, stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return serveRelay(ctx, args, stdout, stderr)
}

func serveRelay(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	cfg := p2p.DefaultRelayConfig()
	fs := flag.NewFlagSet("relay", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	keyPath := fs.String("key", "", "libp2p protobuf private key `file`, not readable by others or writable by the group (required)")
	genKey := fs.Bool("gen-key", false, "write a new ed25519 key to the --key file, print its peer id and exit")
	printID := fs.Bool("print-id", false, "print the peer id of the --key file and exit")
	listen := listFlag{list: &cfg.Listen}
	fs.Var(&listen, "listen", "listen `multiaddr`, repeatable or comma separated")
	fs.Var(&listFlag{list: &cfg.Announce}, "announce", "public `multiaddr` without /p2p to tell peers instead of the listen addresses, repeatable or comma separated")
	fs.IntVar(&cfg.MaxReservations, "max-reservations", cfg.MaxReservations, fmt.Sprintf("maximum reservations held at once (at most %d)", p2p.MaxRelayReservations))
	fs.IntVar(&cfg.MaxReservationsPerIP, "max-reservations-per-ip", cfg.MaxReservationsPerIP, "maximum reservations from one IP address (at most --max-reservations)")
	fs.IntVar(&cfg.MaxCircuits, "max-circuits", cfg.MaxCircuits, fmt.Sprintf("maximum relayed connections per peer (at most %d, and --max-reservations x --max-circuits at most %d)", p2p.MaxRelayCircuits, p2p.MaxRelayCircuitsTotal))
	fs.DurationVar(&cfg.ReservationTTL, "reservation-ttl", cfg.ReservationTTL, fmt.Sprintf("lifetime of a reservation before the peer renews it (at most %v)", p2p.MaxRelayReservationTTL))
	fs.DurationVar(&cfg.MaxDuration, "max-duration", cfg.MaxDuration, fmt.Sprintf("maximum duration of a relayed connection (at most %v)", p2p.MaxRelayDuration))
	fs.Var(sizeFlag{&cfg.MaxData}, "max-data", "maximum `bytes` of a relayed connection in each direction (suffix KiB, MiB or GiB, at most "+formatSize(p2p.MaxRelayData)+")")
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), relayUsage)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fs.SetOutput(stdout)
			fs.Usage()
			return 0
		}
		fs.SetOutput(stderr)
		fmt.Fprintf(stderr, "agent-courier relay: %v\n\n", err)
		fs.Usage()
		return 2
	}
	if fs.NArg() > 0 || *keyPath == "" {
		fs.SetOutput(stderr)
		fmt.Fprint(stderr, "agent-courier relay: --key is required and no arguments are allowed\n\n")
		fs.Usage()
		return 2
	}

	if *genKey {
		id, err := p2p.GenerateRelayKey(*keyPath)
		if err != nil {
			fmt.Fprintf(stderr, "agent-courier relay: %v\n", err)
			return 1
		}
		fmt.Fprintln(stdout, id)
		return 0
	}
	if *printID {
		id, err := p2p.LoadRelayID(*keyPath)
		if err != nil {
			fmt.Fprintf(stderr, "agent-courier relay: %v\n", err)
			return 1
		}
		fmt.Fprintln(stdout, id)
		return 0
	}

	log := slog.New(slog.NewTextHandler(stderr, nil))
	key, err := p2p.LoadRelayKey(*keyPath)
	if err != nil {
		log.Error("load key", "err", err)
		return 1
	}
	r, err := p2p.StartRelay(key, cfg)
	if err != nil {
		log.Error("start relay", "err", err)
		return 1
	}
	defer r.Close()
	log.Info("relay started", "listen", fmt.Sprint(r.Addrs()), "advertised", fmt.Sprint(r.Advertised()),
		"max_reservations", cfg.MaxReservations, "max_reservations_per_ip", cfg.MaxReservationsPerIP,
		"max_circuits", cfg.MaxCircuits, "reservation_ttl", cfg.ReservationTTL,
		"max_duration", cfg.MaxDuration, "max_data", cfg.MaxData)
	if !r.HasPublicAddr() {
		log.Warn("no advertised address is public, members cannot reach the relay through it: set --announce")
	}
	relayStarted(r)

	tick := time.NewTicker(statsInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			if err := r.Close(); err != nil {
				log.Error("stop relay", "err", err)
				return 1
			}
			log.Info("relay stopped")
			return 0
		case <-tick.C:
			st := r.Stats()
			log.Info("relay stats", "peers", st.Peers, "reservations", st.Reservations, "circuits", st.Circuits,
				"reservations_refused", formatCounts(st.ReservationsRefused), "circuits_refused", formatCounts(st.CircuitsRefused))
		}
	}
}

// formatCounts is "NAME=n,..." sorted by name, or "none".
func formatCounts(m map[string]int64) string {
	if len(m) == 0 {
		return "none"
	}
	var b strings.Builder
	for _, k := range slices.Sorted(maps.Keys(m)) {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%s=%d", k, m[k])
	}
	return b.String()
}

// listFlag is a repeatable, comma separated flag. The first use replaces the
// default list.
type listFlag struct {
	list *[]string
	set  bool
}

func (f *listFlag) String() string {
	if f.list == nil {
		return ""
	}
	return strings.Join(*f.list, ",")
}

func (f *listFlag) Set(v string) error {
	if !f.set {
		*f.list = nil
		f.set = true
	}
	for s := range strings.SplitSeq(v, ",") {
		if s = strings.TrimSpace(s); s != "" {
			*f.list = append(*f.list, s)
		}
	}
	return nil
}

// sizeFlag is a byte count with an optional binary suffix.
type sizeFlag struct{ n *int64 }

func (f sizeFlag) String() string {
	if f.n == nil {
		return ""
	}
	return formatSize(*f.n)
}

func (f sizeFlag) Set(v string) error {
	n, err := parseSize(v)
	if err != nil {
		return err
	}
	*f.n = n
	return nil
}

var sizeUnits = []struct {
	suffix string
	shift  uint
}{{"GiB", 30}, {"MiB", 20}, {"KiB", 10}, {"B", 0}}

func parseSize(v string) (int64, error) {
	num, shift := v, uint(0)
	for _, u := range sizeUnits {
		if s, ok := strings.CutSuffix(v, u.suffix); ok {
			num, shift = s, u.shift
			break
		}
	}
	n, err := strconv.ParseInt(strings.TrimSpace(num), 10, 64)
	if err != nil || n <= 0 || n > (1<<62)>>shift {
		return 0, fmt.Errorf("invalid size %q: want a positive integer with an optional KiB, MiB or GiB suffix", v)
	}
	return n << shift, nil
}

func formatSize(n int64) string {
	for _, u := range sizeUnits {
		if n > 0 && n%(1<<u.shift) == 0 {
			return strconv.FormatInt(n>>u.shift, 10) + u.suffix
		}
	}
	return strconv.FormatInt(n, 10)
}
