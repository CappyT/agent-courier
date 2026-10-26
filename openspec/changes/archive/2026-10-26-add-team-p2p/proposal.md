# Proposal

## Why

The agents of a small team work on the same repositories from different machines, behind home and office NATs. They need to exchange the same messages as local agents, with nothing to operate and nobody in the middle who can read or forge them. This change extends the local courier of `add-local-courier` to the members of a team, peer to peer.

## What Changes

- Each running daemon has a peer identity: an ed25519 key it creates in memory with its first team.
- A team is a name, a set of topics and a list of trusted members, held only in memory. A machine joins with a one-time invite code from any member. A team ends when its last member leaves; to exclude a machine, the members create a new team.
- The daemon runs a libp2p host while it runs. It connects directly to other members when it can, and through a relay when NAT traversal fails. Every connection is encrypted and authenticated end to end, so a relay sees only ciphertext.
- Messages on a team topic reach the agents of every member. Agents address remote agents as `<member>/<agent>`. A message for a member whose connection is down waits in the sender's memory and goes out when it reconnects.
- A relay node, the same binary with another subcommand, runs on the personal cluster as the default relay. Public libp2p relays found through the DHT are the fallback.
- **BREAKING** for the local mode: none. Local topics and agents keep working as before.

## Capabilities

### New Capabilities

- `peer-identity`: the daemon's ephemeral key, its peer id, and the member label.
- `team-membership`: ephemeral teams, invite codes, joining, the member list and leaving.
- `p2p-transport`: connectivity between members: direct paths, NAT traversal, relays, discovery, encryption and the access rule for unknown peers.
- `team-messaging`: messages across machines: remote addresses, routing on team topics, the outbox and delivery while peers come and go.
- `relay-node`: the relay that the project runs as the default: limits, identity and what it can see.

### Modified Capabilities

- `local-daemon`: a daemon in a team keeps running without clients, because its teams live only in its memory.

## Impact

- New dependency: `github.com/libp2p/go-libp2p` and its DHT and relay modules.
- The daemon opens UDP and TCP sockets while it runs, only on a machine that belongs to a team, as the `local-daemon` spec allows.
- The personal cluster gets a relay deployment with a public UDP and TCP port and a DNS name.
- The member's IP addresses and peer id are published in the public DHT, and the relay operator sees which peers talk to each other.
- Delivery needs both daemons running at once. A daemon in a team stays up without sessions, and a restart takes the machine out of its teams.
