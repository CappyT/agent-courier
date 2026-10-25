# Tasks

## 1. Spike: reachability behind NAT (#25)

- [x] 1.1 Build a throwaway go-libp2p pair with QUIC, TCP, Noise, AutoNAT, hole punching and a relay, and verify two machines on different home networks exchange a frame
- [x] 1.2 Measure, for the same pair, direct dial, hole punching and the relayed path, and the DHT lookup in client mode, and verify each result with logs
- [x] 1.3 Record the results and the discovery order in design.md, and verify `openspec validate add-team-p2p` passes

## 2. Identity (#26)

- [x] 2.1 Create and load the machine key with mode 0600, and verify a test shows the peer id stays the same across two loads
- [x] 2.2 Verify with a test that a new key file gives a new peer id that the other members refuse

## 3. Teams and roster (#27)

- [x] 3.1 Add the team, topic, member and invite tables, and verify a test creates a team and lists it
- [x] 3.2 Implement the roster merge with a hybrid logical clock, and verify a property test shows any merge order gives the same roster
- [x] 3.3 Implement the `team` subcommands over the daemon socket, and verify a test drives create, list, remove and leave

## 4. Invite and join (#28)

- [x] 4.1 Implement invite codes with a 128-bit secret, a stored hash and a 24-hour expiry, and verify tests cover the expired and the reused code
- [x] 4.2 Implement the join protocol, and verify a test with two in-process hosts covers the scenarios of "Join", including a taken label and an offline inviter
- [x] 4.3 Implement roster propagation on connect and after changes, and verify a test with three hosts covers the scenarios of "Roster propagation" and "Removal"

## 5. Transport (#29: 5.2, 5.3; #30: 5.1, 5.4)

- [x] 5.1 Start the host only for a machine in a team, and verify a test shows a machine with no team opens no network socket
- [x] 5.2 Implement the access rule on the courier protocol, and verify a test resets a stream from a peer outside the roster before it reads a frame
- [x] 5.3 Implement framing with the 64 KiB cap and the per-peer stream cap, and verify a test resets an oversized frame
- [x] 5.4 Implement the relay list, the reservation and the dial order chosen in 1.3, and verify a test with an in-process relay connects two hosts that cannot dial each other

## 6. Team messaging (#31: 6.1-6.3; #32: 6.4)

- [x] 6.1 Route sends on team topics to local sessions and the outbox, and verify tests cover the scenarios of "Remote addresses" and "Team topic delivery"
- [x] 6.2 Implement the outbox worker, backoff, expiry and receipts, and verify tests with an injected clock cover the scenarios of "Outbox" and "Delivery receipt"
- [x] 6.3 Pass remote messages through the local validation, rendering and budgets, and verify a test covers "Same local rules"
- [x] 6.4 Implement presence frames and remote entries in `agents`, and verify a test covers "Remote presence"

## 7. Relay node (#33)

- [x] 7.1 Implement `agent-courier relay` with a key file and limit flags, and verify a test reserves a slot and hits the reservation cap
- [x] 7.2 Add a container image and its publication to the release workflow, and verify the image runs `agent-courier relay --help`

## 8. Relay deployment (#34)

- [x] 8.1 Add the relay Deployment, Secret, LoadBalancer Service on UDP and TCP 4001 and DNS record `relay.cappyt.sh` to the personal cluster in the kube repo, and verify the pod is ready and the port answers from outside the LAN
- [ ] 8.2 Put the relay address in the default relay list, and verify a daemon outside the LAN holds a reservation on it

## 10. Ephemeral trusted teams (#37)

- [x] 10.1 Hold teams, member lists and invites in memory with the join-counter merge of design.md "Member list", and remove the store team tables, entry signatures, join proofs, epochs and the remove command; verify a test that any merge order gives the same list and a leave beats the join it follows
- [x] 10.2 Create the machine key in memory with the first team and never on disk, and keep the daemon running without clients while it is in a team; verify tests for both
- [x] 10.3 Simplify the join protocol and the `members` frame to the design, and verify a three-host test covers the scenarios of "Join", "Trusted members" and "Leave"

## 11. In-memory send queue (#38)

- [x] 11.1 Replace the store outbox and `seen` table with an in-memory send queue and receive dedupe per team, dropped when a member leaves or the team ends; verify tests with an injected clock cover "Send queue" and "Delivery receipt"

## 9. Integration and docs (#35)

- [ ] 9.1 Verify a round trip between two people on different networks: a `handoff` sent while the recipient's machine is disconnected arrives when it reconnects, both agents acknowledge, and both pending lists are empty; record the evidence in the issue
- [ ] 9.2 Write the team part of README.md: commands, what the relay and the DHT see, that a team lives in daemon memory and ends with a restart, and verify the commands run as written
