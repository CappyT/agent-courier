# Tasks

## 1. Spike: reachability behind NAT

- [x] 1.1 Build a throwaway go-libp2p pair with QUIC, TCP, Noise, AutoNAT, hole punching and a relay, and verify two machines on different home networks exchange a frame
- [x] 1.2 Measure, for the same pair, direct dial, hole punching and the relayed path, and the DHT lookup in client mode, and verify each result with logs
- [x] 1.3 Record the results and the discovery order in design.md, and verify `openspec validate add-team-p2p` passes

## 2. Identity

- [x] 2.1 Create the machine key in memory with the first team and never write it to disk, and verify a test shows a restarted daemon has a new peer id and an empty config directory

## 3. Ephemeral trusted teams

- [x] 3.1 Hold teams, member lists and invites in daemon memory, and verify a test shows a restarted daemon has no team
- [x] 3.2 Merge member lists by join counter with a leave beating the join it follows, and verify a property test shows any merge order gives the same list
- [x] 3.3 Implement the `team` subcommands create, invite, join, list and leave over the daemon socket, and keep the daemon running without clients while it is in a team; verify tests for both

## 4. Invite and join

- [x] 4.1 Implement invite codes with a 128-bit secret, a hash held in memory and a 24-hour expiry, and verify tests cover the expired and the reused code
- [x] 4.2 Implement the join protocol, and verify a three-host test covers the scenarios of "Join", including a taken label, a rejoin after a restart and an offline inviter
- [x] 4.3 Send member lists on connect, after changes and on every resync tick, and verify a three-host test covers "Trusted members" and "Leave"

## 5. Transport

- [x] 5.1 Start the host only for a machine in a team, and verify a test shows a machine with no team opens no network socket
- [x] 5.2 Implement the access rule on the courier protocol, and verify a test resets a stream from a peer outside the member list before it reads a frame
- [x] 5.3 Implement framing with the 64 KiB cap and the per-peer stream cap, and verify a test resets an oversized frame
- [x] 5.4 Implement the relay list, the reservation and the dial order chosen in 1.3, and verify a test with an in-process relay connects two hosts that cannot dial each other

## 6. Team messaging

- [x] 6.1 Route sends on team topics to local sessions and the in-memory send queue, and verify tests cover the scenarios of "Remote addresses" and "Team topic delivery"
- [x] 6.2 Implement the send queue worker, backoff, receipts and the receive dedupe, dropped when a member leaves or the team ends, and verify tests with an injected clock cover "Send queue" and "Delivery receipt"
- [x] 6.3 Pass remote messages through the local validation, rendering and budgets, and verify a test covers "Same local rules"
- [x] 6.4 Implement presence frames and remote entries in `agents`, and verify a test covers "Remote presence"

## 7. Relay node

- [x] 7.1 Implement `agent-courier relay` with a key file and limit flags, and verify a test reserves a slot and hits the reservation cap
- [x] 7.2 Add a container image and its publication to the release workflow, and verify the image runs `agent-courier relay --help`

## 8. Relay deployment (#39)

- [x] 8.1 Add the relay Deployment, Secret, LoadBalancer Service on UDP and TCP 4001 and DNS record `relay.cappyt.sh` to the personal cluster in the kube repo, and verify the pod is ready and the port answers from outside the LAN
- [ ] 8.2 Put the relay address in the default relay list, and verify a daemon outside the LAN holds a reservation on it

## 9. Integration and docs (#40)

- [ ] 9.1 Verify a round trip between two people on different networks: a `handoff` sent while the recipient's machine is disconnected arrives when it reconnects, both agents acknowledge, and both pending lists are empty; record the evidence in the issue
- [ ] 9.2 Write the team part of README.md: commands, what the relay and the DHT see, that a team lives in daemon memory and ends with a restart, and verify the commands run as written
