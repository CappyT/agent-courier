# Design

## Context

This change builds on `add-local-courier`: one daemon per user, started on demand, owns validation, rendering, budgets, storage and push. See `proposal.md` for the motivation and `specs/` for the behavior. Three facts shape the design:

- Members sit behind home and office NATs. Hole punching fails behind symmetric NATs, so a relay is needed for some pairs.
- A team lives only in the memory of its members' daemons. A daemon in a team keeps running without sessions, and a daemon that exits forgets its teams.
- Members are invited by a member, and invited members are trusted for identity and membership. The threat model covers strangers on the network and the relay operator, not members. Text from a member is still data, and budgets still cap what it pushes into a session, because context is scarce, not because the member is hostile.
- go-libp2p (v0.51, pre-1.0) gives Noise, QUIC and TCP transports, AutoNAT, hole punching (DCUtR), circuit relay v2 and a Kademlia DHT in Go, with no CGO. Its public relays limit each relayed connection to 2 minutes and 128 KiB by default.

## Goals / Non-Goals

**Goals:**

- No server that can read or forge a message. A relay forwards Noise ciphertext.
- Nothing to operate for a team that uses the project relay.
- Remote messages follow the same rules as local ones, in the same code.

**Non-Goals:**

- Delivery after a team ends, or to a member whose daemon restarted: it is out of the team.
- Defense against a member: a member can already send anything a member may send. Removing a member is done by creating a new team.
- Teams larger than about 20 members. Every message goes to every member over its own stream.
- Protection of metadata. The DHT and the relay see peer ids, IP addresses and traffic volume.
- Forward secrecy between messages beyond what Noise gives per connection.

## Decisions

### Host

The daemon starts one libp2p host when the machine creates or joins its first team, and closes it when it leaves its last team. It creates the machine key in memory at that moment. A machine without a team opens no network socket. The host listens on every interface, QUIC and TCP, IPv4 and IPv6, on ports the system picks. Transports: QUIC and TCP, security: Noise, muxer: yamux. It enables the AutoNAT v2 client, hole punching and the circuit relay client. It keeps identify address discovery, which the relay node turns off, because hole punching needs the addresses that peers observe. It does not use AutoRelay, because AutoRelay stops reserving once reachability is public: the daemon holds its reservation itself and renews it when three quarters of its life have passed. The libp2p loggers write peer ids, so their records reach the daemon log at debug level only.

The resource manager has fixed per-peer limits: 4 inbound and 8 outbound connections, 32 inbound and 64 outbound streams. `/agent-courier/msg/1.0.0` allows 4 streams per peer in each direction and 256 in all. `/agent-courier/join/1.0.0` allows 1 stream per peer and 16 in all. The daemon closes an inbound connection, direct or relayed, from a peer outside every member list that has no join stream open 5 seconds after it opens, which frees the circuit slots it holds on the relay.

- Alternative: iroh. Rejected: its Go bindings need CGO or are immature, and its public relays are for development only.
- One member set: the daemon holds its teams in memory, and the host's stranger rule, the access rule and the messaging code read that one structure.
- Circuits: a dial to a member uses only the relays of the teams this machine shares with that member.
- The host serves no AutoNAT v2 dial-request: it runs the client only.
- Reservations: each relay list reserves on its own, in parallel. After a reservation is stored, the loop checks the relay is still connected.
- Addresses: the dialer drops `/unix`, loopback, unspecified, link-local and multicast addresses, including `/dns*/localhost` and names under `.localhost`; private ranges stay, because members on one LAN use them. Join dials invite addresses through the same filter.

### Access rule

A connection gater lets any peer connect, because relay and DHT traffic need it. The courier protocols check membership instead: the stream handler for `/agent-courier/msg/1.0.0` resets a stream whose remote peer id is not in the member list of a team the daemon shares, before it reads a byte. A frame that names a team is accepted only from a member of that team. The join protocol `/agent-courier/join/1.0.0` is the only exception, and it accepts only a valid invite secret. Noise authenticates every peer id, so a stranger cannot pass as a member.

### Wire protocol

Frames on a stream are a 4-byte big-endian length, then a JSON object, with a cap of 64 KiB per frame. Frame types:

- `message`: id, topic, sender agent name, recipient, kind, fields. The receiver takes the member label from its member list entry for the connection's peer id.
- `receipt`: message id.
- `members`: the member list of one team, each entry a peer id, label, addresses, join counter and a left flag. It goes out in as many frames as its entries need.
- `presence`: the agents online on the topics both members share. A recipient holds at most one pending online per member and topic, a new one replacing the earlier one, and gets at most 30 onlines per member per hour pushed. `agents` lists every local agent before remote ones, and shows at most 20 agents per member and topic. These caps keep the session context small, they are not a defense.

A sender opens a stream, writes frames, reads receipts and closes. Streams are short, so the default limits of public relays hold.

### Identity and storage

- The machine key is an ed25519 key held in memory from the first team until the daemon exits. It is never written to disk, so there is no key file to protect, and a restarted daemon is a new peer.
- Teams, member lists, invites, the send queue and the receive dedupe live in memory. The SQLite store keeps only what the local mode keeps: a remote message delivered to a local agent is stored like a local message, with its sender `<label>/<agent>` and the peer id as its origin, and follows the local retention.

### Member list

The member list of a team is a map from peer id to an entry: label, addresses, a join counter and a left flag. Members are trusted, so entries carry no signatures.

- The join counter is a clock value: the inviter sets it to the larger of one more than the counter it holds for that peer id and the current Unix time in milliseconds. A member sets its own entry the same way when its addresses change. Merge by peer id keeps the entry with the higher counter, then the left entry, then label, addresses and peer id in byte order, so every merge order gives the same list. A join after a leave always carries a later counter, so a rejoin wins over the leave without any repair step.
- A daemon merges a `members` frame only from a member of the team it names. A merge that adds or drops a member updates the member set at once.
- A daemon sends the member list of a team on every connection to a member of that team, after each local change, after a merge that changed the list (to every member but the sender), and again on every resync tick, one stream per member for every team that member shares. Members frames have no rate limit: they never reach a session, and members are trusted.
- Rejoin with the same label: a restarted daemon is a new peer id, and its user rejoins with the label it had. When the label of a join belongs to a member that is not connected to the inviter, the inviter marks that member left in the same step and admits the joiner; when that member is connected, the join gets `label_taken`. A daemon that stops on a signal or a context cancel leaves every team first, within the leave timeout.
- Leave: the daemon marks its own entry left, sends the list to every member it can reach, and forgets the team. A member that missed the notice learns it from another member's list. A team ends when no member is left.
- A team holds at most 64 members and a machine follows at most 16 teams. An entry carries at most 4 addresses of at most 256 bytes.
- Alternative: signed entries with join proofs and epochs, so a member cannot forge the list. Rejected: it defended the list against members who are trusted by definition, and every rule opened a new edge case. Excluding a machine means creating a new team.

### Invite and join

An invite code is the base32 text of: team name, the inviter's peer id, its addresses including its circuit address on the project relay, a 128-bit secret and the expiry. The inviter keeps only a hash of the secret, in memory. The joiner dials the inviter by peer id, so Noise authenticates the inviter, and sends the secret and its label on the join protocol. The inviter takes the joiner's peer id from the authenticated connection, never from the frame.

- Code: RFC 4648 base32, lower case, without padding; the joiner ignores case and the white space around it. Version 1 is binary: the version byte 1; the team name and the binary peer id, each a 1-byte length and its bytes; the 16-byte secret; the expiry in Unix seconds, 8 bytes; the address count, 1 byte; each address as a 2-byte length and its binary multiaddr, without a `/p2p` part. Integers are unsigned big-endian. At most 4 addresses and 2048 characters. A code with bytes left over, or a field that breaks its rule, is refused.
- Invites: a code works once, for 24 hours, and dies when its inviter leaves the team or its daemon exits. A team holds at most 16 open invites per member.
- Frames, on one stream: the joiner sends `join` with the team, the secret and its label. The inviter checks the secret, that it is still a member, the label and the cap, and replies `offer` with the topics, or a refusal (`refused`, `label_taken`, `team_full`). The joiner checks the topics against its own teams and sends `join_accept`, or closes and the code stays valid. The inviter marks the code used, adds the joiner with its join counter, sends the member list and closes. Then both daemons send the list to the other members.
- One refusal, `refused`, covers a wrong, used or expired secret and a team the inviter does not hold, so a guesser learns nothing. The other refusals come only after a valid secret.
- The join handler serves at most 8 join streams at once and 1 per peer, and resets the others before it reads a byte. A frame read or write has 10 seconds, a stream 1 minute. Errors and logs never quote a frame or a code.
- The joiner refuses the join when another of its teams owns one of the topics. A topic belongs to one team.
- An offline inviter: the dial fails within 30 seconds, the join fails with -32031.

### Discovery and reachability

1. Every member keeps a reservation on the first reachable relay of each team's list. A relay that two lists name holds one reservation for both. A list without a reservation is retried with backoff from 5 seconds to 5 minutes, and a lost connection to the relay starts a new reservation at once. Its circuit address, `<relay list entry>/p2p-circuit/p2p/<own peer id>`, is built from the list entry and goes into its member entry.
2. A daemon dials a member on the addresses of its member entry, only after its own reservation is up and after a bounded wait for a public observed address: at most 10 seconds after the host starts. The signal for that address is the DCUtR handler: the hole punching service registers it when the host knows a public address. One dial takes direct addresses first, then the circuit addresses: the member's own, and one through each relay of the daemon's lists. The dial ranker delays the circuit by 500 ms behind every direct address, private ones too; the default ranker delays it only behind public ones, so a relayed connection could win against a member on the same LAN. Hole punching upgrades a relayed connection to a direct one when it can, and the relayed connection stays when it cannot.
   - The daemon drops `/unix` addresses and loopback addresses from a member entry: a loopback address in another machine's entry names the dialing machine, not the member. Two members on one machine reach each other through their LAN or relay addresses.
   - Every team uses the default relay list: the project relay over QUIC and TCP. Its peer id comes from the relay deployment. A per-team relay list is not part of 0.5.0.
3. When no address works, the daemon asks the DHT, in client mode, for the member's peer record.


### Measured reachability

A throwaway go-libp2p v0.51.0 pair (QUIC, TCP, Noise, yamux, AutoNAT v2, hole punching, AutoRelay) exchanged one length-prefixed JSON frame. Real network: the workstation behind the home NAT and a pod on the personal cluster. The pod is behind the node SNAT on the same LAN and the same public IP, so it is not a second home network. Simulated: two Linux NAT containers with a relay on a public-range bridge.

- Direct dial, pod to workstation on the LAN: 5 ms on QUIC and on TCP. Workstation to pod: fails after 5 s, the node SNAT drops inbound traffic.
- Relayed path, own relay on the LAN: connect 11 ms, frame round trip 2 ms, the relay grants the configured 10 min and 1 MiB. Public IPFS relay found through DHT peers: reservation 5.3 s after start, cold connect 1.5 s, frame round trip ~285 ms, limits 2 min, 128 KiB and a 1 h reservation.
- One connect with all member addresses gives the order of step 2: the dial ranker tries direct addresses first, then the circuit. With only private direct addresses it starts the circuit at once.
- DCUtR, simulated port-preserving NAT, 50 ms RTT: upgraded 3/3 on QUIC (punch 103 ms, direct 455 ms after the dial) and 2/2 on TCP (202 ms, 655 ms). Random-port NAT: 0/3, three 5 s attempts, the relayed connection keeps working.
- DCUtR, real network: started on both sides and failed 3/3. Both peers share one public IP and the router does not hairpin to it (TCP refused, QUIC timeout). Hole punching needs a public observed address, so it never starts on a LAN-only path.
- DHT client mode, public DHT: bootstrap 1.7-2.4 s. FindPeer of a client-mode peer 2.1 s, provider lookup 0.13 s, provide 16 s. Both returned the circuit and public addresses.

The transport must meet two conditions the spike found:

- AutoRelay publishes a circuit address only for a relay that announces a public address. The relay node announces its public address, and the daemon builds its circuit address from the relay list entry, not from the host addresses.
- DCUtR starts only when both peers know a public observed address before the relayed connection opens. A peer that dialed before its first exchange with the relay got `protocols not supported: [/libp2p/dcutr]` and stayed relayed. A host learns a public observed address from 4 distinct observers, and a reservation alone does not give one. So the daemon dials members after its reservation is up and after a bounded wait for that address. A member that disables the DHT has few observers and, in practice, no hole punching: its connections stay relayed.

Not measured: hole punching between two real home networks, CGNAT, IPv6, relay limits on the real network (they are tested in process), and the DHT with members that churn. The dial order above holds.

### Outbox

`send` on a team topic delivers to local sessions and queues one item per other member, in memory. A worker sends due items with backoff from 5 seconds up to 10 minutes, and deletes an item on its receipt. When a member connects, its items become due at once. One pass takes at most 32 items per member and skips a member with a send in flight, so one member's backlog never delays the others. The items for a member are dropped when it leaves, and all items of a team when the team ends or the daemon exits.

- The receiver records `(peer id, message id)` of each stored message in memory for 24 hours, sends a receipt for a duplicate, and stores it once. A sender retries every 10 minutes at most, so a lost receipt is answered long before the record goes.
- The send queue holds at most 1000 items per member, so a local agent stuck in a loop cannot fill memory. A `send` to one agent of a full member fails with -32014 and names the member. A broadcast still reaches the other members and lists the full one under `skipped`, as the local pending cap does.
- The wire id of a remote message lives as long as its dedupe record, 24 hours. A reply to a remote ask older than that carries the local id and the asker cannot match it.
- A remote message keeps its wire id beside its local id, so a reply to a remote ask carries the wire id back.
- Held messages are ordered by their local receive time, then id.

### Relay node

`agent-courier relay` runs a circuit relay v2 with limits raised above the libp2p defaults: 10 minutes and 1 MiB per relayed connection (chosen), with flags to change them. It runs on the personal cluster as a Deployment with one replica and the DNS name `relay.cappyt.sh`. The release workflow publishes the image `ghcr.io/cappyt/agent-courier:<version>` to GHCR as a public package.

- Addresses: the relay runs with `--announce /dns4/relay.cappyt.sh/udp/4001/quic-v1,/dns4/relay.cappyt.sh/tcp/4001`. It refuses an announce address that is not public or that carries `/p2p` or `/p2p-circuit`, and logs the addresses it advertises. Identify address discovery is off, so peers cannot make it advertise an observed address.
- Source IP: the LoadBalancer Service with UDP and TCP on port 4001 uses `externalTrafficPolicy: Local`. Behind a node SNAT every client shares one IP, and the per-IP limits would cap the relay at about 8 members.
- Key: a Secret mounted read-only with `defaultMode: 0440`, and the pod runs as uid and gid 65532 with `fsGroup: 65532`, non-root, a read-only root filesystem, no privilege escalation, all capabilities dropped and the RuntimeDefault seccomp profile. The relay refuses a key file that others can read or the group can write.
- Limits: 1024 reservations by default, 8 per IP, 16 circuits per peer. A memory limit of 1 GiB on the pod. `agent-courier relay --print-id` prints the relay's peer id for the relay list.
- Default relay list: `/dns4/relay.cappyt.sh/udp/4001/quic-v1/p2p/12D3KooWEBZoLrJa7yXYJPH5SuHmwWtDCFA39ntvvp883rte7qde` and `/dns4/relay.cappyt.sh/tcp/4001/p2p/12D3KooWEBZoLrJa7yXYJPH5SuHmwWtDCFA39ntvvp883rte7qde`. The key behind this peer id is in Infisical, project `personal-k8s`, environment `prod`, as base64 in `/agent-courier/RELAY_KEY`. The manifests are in `clusters/personal/agent-courier/` of the kube repo, and the Service holds the dedicated public IP 185.242.182.147 (freed when warpgate moved behind the eden gateway) with `externalTrafficPolicy: Local`, so the relay sees each client's own address. Cilium 1.20 LB-IPAM shares an IP between Local Services only when their selectors are equal, so the relay cannot share a gateway IP; `lbipam.cilium.io/sharing-permit-different-pods` (Cilium 1.21) would allow it.
- The relay serves circuit relay v2 and identify only: no ping, no DHT, no AutoNAT, no relay client.

### CLI

`agent-courier team create|invite|join|leave|list` talk to the daemon over the local socket and start it when needed. A session calls the team methods without `register`, because the CLI is not an agent. Errors name the rule and never quote a value the client sent, except a team name that passed its rule. A team name is `^[a-z0-9][a-z0-9_-]{0,63}$`, a member label is `^[a-z0-9][a-z0-9-]{0,31}$`.

A team in a result:

```json
{"name": "courier", "topics": ["cappyt_agent-courier"], "members": [{"label": "alpha", "peer_id": "12D3KooW..."}], "members_total": 1}
```

A result lists at most 64 members of a team, and `team.list` without a team at most 10 per team, so 16 teams stay under the line cap.

| Method | Params | Result |
| --- | --- | --- |
| `team.create` | `team`, required. `topics`, 1 to 16 topics, required. `label`, required. | `{"team": <team>}`, with this machine as the only member. |
| `team.list` | `team`, optional. | `{"teams": [<team>...]}`, sorted by name; with `team`, only that team. |
| `team.leave` | `team`, required. | `{"text": "Left team courier."}`. The daemon tells the members it can reach and forgets the team. |
| `team.invite` | `team`, required. | `{"invite": {"team": <name>, "code": <code>, "expires": <RFC 3339 time, UTC>}}`. No log holds the code. |
| `team.join` | `code`, required, at most 2048 characters. `label`, required; the CLI fills in the host name. | `{"team": <team>}` after the join, within 90 seconds. |

| Code | Meaning | When |
| --- | --- | --- |
| -32602 | invalid params | A name, label, topic or invite code breaks its rule, or `topics` is empty or has more than 16 topics. |
| -32020 | no team | This machine belongs to no team with the name. |
| -32021 | team exists | `team.create` or `team.join` names a team this machine holds. |
| -32022 | topic taken | A topic of `team.create` or of the team of `team.join` is a topic of another team. |
| -32027 | too many teams | `team.create` or `team.join` on a machine that follows 16 teams. |
| -32029 | invite refused | The inviter refused the code: wrong, used, expired or for another team. |
| -32030 | invite expired | The expiry in the code has passed. The joiner checks it before it dials. |
| -32031 | inviter unreachable | The joiner cannot reach the inviter. |
| -32032 | label taken | Another member of the team has the label of `team.join`. |
| -32033 | team full | The team has 64 members. |
| -32034 | too many invites | This member holds 16 open invites for the team. |
| -32036 | join failed | The inviter broke the join protocol. The joiner keeps nothing. |

The CLI shows -32601 from the daemon as a daemon older than the command, which the user stops so the next command starts the new one.

## Risks / Trade-offs

- [A team lives in memory] -> A daemon crash or a reboot takes the machine out of every team, and its queued messages are lost. The user joins again with a new invite. This is the price of having no state to protect.
- [Both daemons must run at once] -> A daemon in a team keeps running without sessions, so a member stays reachable while its machine is on.
- [A member is trusted] -> A member can rename itself, invite anyone or flood within the budgets. Excluding it means a new team. Budgets and pending caps still keep each session's context small.
- [The project relay is a single point of failure for NATed pairs] -> Public relays found through the DHT are the fallback. A per-team relay list can come later.
- [go-libp2p is pre-1.0 and its API changes between minor versions] -> Pin the version, and keep libp2p calls inside one package.
- [The public DHT publishes members' IP addresses] -> Accepted and stated in the README. Disabling the DHT lookup is left for later.
- [A few IP addresses take every relay reservation] -> 1024 reservations at 8 per IP, and existing reservations stay. Not solved for a botnet.
- [Any peer can hold a member's circuit slots on the relay] -> The daemon closes connections from peers outside every member list within 5 seconds, which frees the slots.
- [A member that left keeps the messages it received] -> Accepted: leaving stops future messages, it cannot recall past ones.

## Migration Plan

None: teams live in memory, so the store schema of the local mode does not change, and a machine with no team behaves as before.
