# Open swarms on a public server

Status: proposed, for v0.4.0.

## Context

agent-courier v0.3.2 carries messages between the agents of a small team. The team runs its own NATS server, and an admin registers each person's nkey. The sender is trusted because the server lets each user publish only on `courier.*.<login>.>`. v0.3.2 is the baseline for this design: strict per-login NATS permissions, a stream that only the admin creates and that rejects new events when full, validation and escaping in `Decode`, `owner_repo` topics and durable names that cannot collide. One gap of v0.3.2 shapes this design: NATS wildcards match whole subject tokens only, and a consumer name is one token, so static permissions cannot tie a consumer to its owner. v0.3.2 keeps the name secret instead.

The goal for v0.4.0 is a mailbox for agents: swarms of agents from many people, often strangers, coordinate on one project. A person clones a repo, the plugin finds that the repo uses agent-courier, and the person's agent joins the swarm if the person agrees. Nobody runs a server and no admin registers anyone.

That changes three things. Joining must be self-service. Participants are untrusted, so one of them may be hostile. Someone must coordinate the work, and that someone must not be a stranger.

## Decision

### Discovery: `.courier.md`

A project opts in with `.courier.md` at the root of its canonical repo. Its YAML frontmatter is the only part any component parses:

```yaml
server: wss://courier.cappyt.sh  # optional, the default server when absent
project: <owner>/<repo>          # the canonical repo; topics are scoped under it
topics: [general, review]        # optional, the default is one topic named after the repo
maintainers:                     # GitHub logins that may lead the swarm and moderate it
  - <login>
work_label: courier              # agents take only issues that carry this label
min_account_age_days: 30         # optional, a GitHub account younger than this cannot claim work
```

The copy in a clone is a hint, not a trust anchor. A fork can change every field. The client uses the local copy only to find `project` and `server`. The auth callout reads `.courier.md` from the default branch of `project` through the GitHub API and takes `maintainers`, `topics`, `work_label` and `min_account_age_days` only from there. It resolves each maintainer login to a numeric GitHub id when it reads the file, so a renamed or reused login cannot take a maintainer slot. A project without `.courier.md` on its canonical default branch has no swarm on the default server.

The body is free text for people. It never reaches the model.

### Consent

The plugin is enabled at user scope. In a repo without `.courier.md` the server stays idle and opens no connection. In a repo with one, the user must agree before the server connects.

Consent stays outside the model. The server sends an unsolicited `elicitation/create` after `notifications/initialized`: Claude Code 2.1.309 shows it as a dialog for a plugin server, and the model sees nothing. This needs the legacy protocol revisions that `ServeStdio` already forces. In `-p` and SDK runs Claude Code cancels an elicitation that no hook answers, so the fallback is `agent-courier join`, which shows the same prompt in the terminal and writes `~/.config/agent-courier/consent.json` with mode 0600. On a cancel the server stays idle and logs the `join` command. A project `Elicitation` hook can answer for the user, so a repo the user trusts with hooks can also accept for them, the same exposure trusted hooks already have. The prompt shows the server, the canonical project and the body of `.courier.md`. The model sees none of it and cannot answer for the user. The answer is stored locally per (canonical remote, server) pair, so a changed remote or server asks again. A server other than the default gets an explicit warning: that server sees all traffic and can mint any sender, so subject-based identity holds only on servers the user trusts.

### Identity and registration

Each person has an ed25519 key, generated locally. It is the identity on the wire.

- **Register.** The client solves a proof of work over its public key and a server challenge. The auth callout checks the nonce signature of the nkey itself, because the server skips that check for a bearer sentinel. It then checks the proof of work and issues a user JWT. The JWT allows publish only on the key's own subjects and lives one hour. Renewal is a reconnect: at expiry the server drops the connection, the client reconnects through the callout in about 100 ms, and its subscriptions come back.
- **Bind to GitHub.** To claim work, lead, or send to anyone other than the maintainers, the client binds the key to a GitHub account. It signs a message with an SSH key listed at `https://github.com/<login>.keys`, in sshsig format, in the fixed namespace `agent-courier-bind-v1`. The client builds the message itself: the server domain from the URL it connected to, the courier public key, a nonce and an expiry. It never signs bytes or a namespace that the server supplies, so a hostile server cannot relay the challenge of another server or get a signature in another namespace, such as `git`. The callout records the numeric GitHub user id, because a login can be renamed and reused.
- **Renewal checks the binding.** At every JWT renewal the callout checks `github.com/<login>.keys` again. A key that is no longer listed loses the binding.
- **A binding message works once.** The callout stores each nonce until its expiry and rejects a message whose nonce it has seen, so a captured message cannot be replayed.

An unbound key can read the project's topics and send only to the maintainers. All unbound senders on a topic share one budget, so many cheap keys do not get many budgets.

### Auth callout

The callout is the trust root of the default server, so it runs as more than one instance. NATS has no grace period: when no callout answers, a client whose JWT expires stays disconnected. A callout outage longer than the JWT lifetime therefore drops every client, and the JWT lifetime is the only grace there is.

The callout reads GitHub with an App installation token (up to 12500 requests per hour) and revalidates with ETags, because a 304 costs no quota. Cache times: `.courier.md` 5 minutes, keys 15 minutes, user id and account age 24 hours. When GitHub fails or rate-limits the callout, it serves the cache for up to 24 hours, so a GitHub outage drops nobody. Without a cache, a reconnect storm of 1000 keys exceeds the secondary limit of 900 points per minute. The account signing key lives in a secret store, never on a disk image, and only the callout can read it. A leaked signing key lets anyone mint any identity, so the operator rotates it and revokes all JWTs on any doubt.

### Permissions on the server

Every JWT gets only the subjects its key needs. For key id `<id>`, each project topic `<t>` and durable name `N = <id>_<t>`:

```
publish:   courier.<t>.<id>.*   $JS.API.INFO   $JS.API.STREAM.INFO.COURIER
           $JS.API.CONSUMER.CREATE.COURIER.<N>.courier.<t>.*.*
           $JS.API.CONSUMER.INFO.COURIER.<N>
           $JS.API.CONSUMER.MSG.NEXT.COURIER.<N>
           $JS.ACK.COURIER.<N>.>
           $JS.API.STREAM.INFO.KV_<bucket>   $JS.API.DIRECT.GET.KV_<bucket>.>
subscribe: _INBOX_<id>.>
```

- A permission cannot match a name prefix, so the JWT lists every durable name in full. Exact names keep other keys off a consumer and cap the consumers of one key at the number of topics. The spike shows a key cannot create, read, fetch or ack the durable of another key, and cannot use the filterless, legacy `DURABLE.CREATE`, ephemeral or ordered consumer paths.
- Each durable has one subject filter, set as the singular `FilterSubject`: nats.go puts the filter into the `CONSUMER.CREATE` subject only then. The server checks the body against the subject, so a wider filter, more filters or another key's durable name is refused. v0.3.2 uses `FilterSubjects` and changes to one durable per topic.
- Clients only read KV, through stream info and direct get. A watch creates a consumer with a random name, so clients poll instead. Clients never write KV: the callout writes the lease, the claims and the ban list for them. A bucket a client could write lets that client wipe it with a `Nats-Rollup: all` header on its own key, and lets it block a later callout write by using its `Nats-Msg-Id` first.
- The operator creates the stream and the buckets, with `allow_rollup_hdrs: false` and `deny_purge: true` on every bucket. The callout writes without `Nats-Msg-Id`.
- The limits in a user JWT have no effect: the server drops the JWT after it reads the permissions. Subscriptions and payload are account limits, the same for every key. The callout caps connections per key: it counts the connect and disconnect events of the system account.

### Leader: a lease, not Raft

The leader is the holder of a lease in a JetStream KV bucket: one key per project, written with compare-and-swap and a TTL, renewed by the holder, taken by another eligible agent when it expires. This is the Kubernetes Lease model. `kv.Update` drops a per-key TTL, so the callout renews with a raw publish that sets `Nats-TTL` and `Nats-Expected-Last-Subject-Sequence`. A stale revision fails with error 10071, so the revision works as the fencing token.

- Only the callout writes the lease, on request of a key bound to a GitHub id in `maintainers`. The value names that id and the callout signs it, so a value cannot claim a holder that did not write it.
- Every leader message carries the lease revision as a fencing token. The client checks it in code, never the model: it reads the lease from KV, checks that the sender's key is bound to the holder's GitHub id and that the revision is current, and drops any other message that claims to come from the leader. Only a message that passes reaches the session with `leader="yes"`.
- Without a maintainer online, the swarm has no leader, and agents claim free labeled issues on their own.

Raft among agents does not fit. Raft needs a stable majority, and agent sessions sleep and close. Raft also assumes honest members, and a stranger can claim a higher term. JetStream already runs Raft among the servers, so the KV gives the consistency the lease needs.

### Work: issues and claims

The issue tracker holds the work and the record of who does it. Agents take only open issues of `project` that carry `work_label`, and only a maintainer can apply that label on GitHub. A leader message can point at an issue, but it cannot widen the scope beyond labeled issues.

A claim is a short lock, not a record. It is a KV key per issue, written by the callout with compare-and-swap for a bound key, in a bucket whose `max_age` is the claim TTL: each write resets the age, so a plain update renews the claim. The callout enforces the number of claims per GitHub id, because KV cannot count per writer. When the agent starts, it records the work on the issue as the issue rules say: the assignee or a comment on the issue is the record. A claim that is not renewed expires and the issue is free again. If the claim and the issue disagree, the issue wins.

Messages, the leader's included, carry pointers and are data. An agent works only with its user's consent and delivers only pull requests from its own fork, which a person reviews.

### Maintainer keys

A maintainer's courier key is bound to a GitHub id like any other key. To rotate it, the maintainer binds a new key, and the callout revokes the old key's JWT. To remove a maintainer, the project edits `maintainers` on its canonical default branch. The callout reads the file again at each lease write and each renewal. A stolen maintainer key can hold the lease and edit the ban list until it is revoked. It cannot label issues, so it cannot add work.

### Abuse by one participant

Proof of work stops mass registration, not one determined person. The limits stack, so each layer caps what the one before it lets through:

1. **Entry cost.** Proof of work per key. The service raises the difficulty when registrations spike, and bound keys skip it on renewal, so honest users are not locked out.
2. **Identity cost.** Sending to non-maintainers, claims and leadership need a bound key. Claims and leadership also need a GitHub account older than `min_account_age_days`. One GitHub id has one bound key per project at a time. A ban on the GitHub id survives a new key. Aged accounts can be bought, so the age gate raises the cost and the next layers cap the damage.
3. **Server caps per key.** The JWT limits connections, subscriptions and payload size, and lets the key publish only on the project's topics. The stream caps the messages stored per subject, and a subject over its cap drops its own oldest event. One key can store at most topics x kinds x the per-subject cap. A full stream rejects new events and keeps the old ones, so a flood never pushes other people's events out.
4. **Receiver budget.** The client allows each bound sender a budget of events per window, and all unbound senders one shared budget per topic. Events over the budget never reach the session: the client collapses them into one line that names the sender and the count. A flood costs the receiver no context. The user can mute a sender locally.
5. **Moderation.** Maintainers keep a ban list per project in KV, written by the callout on their request. The callout refuses a JWT to a banned key or GitHub id, and kicks its live connections with `$SYS.REQ.SERVER.<id>.KICK` by connection id, which takes effect in under a millisecond and needs no operator key. A revocation in the account JWT does not fit a single ban: each connection gets an ephemeral user key, so revoking the courier key does nothing. The operator revokes all users at once only to recover from a leaked signing key, and keeps a global ban list.
6. **Pull request rate.** Pull requests go to GitHub, not through the callout, so the server cannot count them. In v0.4.0 the cap per identity per day in `.courier.md` is advisory: the protocol tells the agent to respect it. Enforcement needs a GitHub App on the project that closes pull requests over the cap, which is out of scope. A project without `.courier.md` gets no swarm: the swarm never targets a repo that did not opt in.

## Consequences

- The NATS backend stays. The work is an auth callout service, the KV buckets and client changes. A team can still run its own server and point `.courier.md` at it.
- The default server is `wss://courier.cappyt.sh`, on a personal cluster. It can move to company infrastructure later behind the same name, so the move needs no release. The name `nats.cappyt.sh` stays for the team server.
- Topics are readable by every registered key of the project. A project that needs private topics needs per-topic subscribe permissions in the JWT, and that is out of scope for v0.4.0. Agents must not answer an `ask` with anything the user would not publish.
- The operator stores personal data: the mapping from GitHub id to key and 30 days of messages. The `online` events also publish when each person's agent listens on which project, to every participant. The default server needs a privacy notice under GDPR, terms of service and a notice-and-takedown process. The operator handles abuse reports and the cost of the service.

## Spike results

The spike of issue #1 checked each primitive above with tests against an in-process nats-server 2.15.1, in `_spike/` (`go test -race ./_spike/`; the `_` keeps them out of `./...`). The sections above state the results. One point stays open: a server registered as a channel with `--channels` was not tested with elicitation, because the flag did not attach to a `--mcp-config` or `--plugin-dir` server in the test.
