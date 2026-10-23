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

Consent stays outside the model. The server asks through MCP elicitation, or the user runs a CLI command. The prompt shows the server, the canonical project and the body of `.courier.md`. The model sees none of it and cannot answer for the user. The answer is stored locally per (canonical remote, server) pair, so a changed remote or server asks again. A server other than the default gets an explicit warning: that server sees all traffic and can mint any sender, so subject-based identity holds only on servers the user trusts.

### Identity and registration

Each person has an ed25519 key, generated locally. It is the identity on the wire.

- **Register.** The client solves a proof of work over its public key and a server challenge. The auth callout checks it and issues a user JWT. The JWT allows publish only on the key's own subjects and lives one hour. The client renews it through the callout.
- **Bind to GitHub.** To claim work, lead, or send to anyone other than the maintainers, the client binds the key to a GitHub account. It signs a message with an SSH key listed at `https://github.com/<login>.keys`, in sshsig format, in the fixed namespace `agent-courier-bind-v1`. The client builds the message itself: the server domain from the URL it connected to, the courier public key, a nonce and an expiry. It never signs bytes or a namespace that the server supplies, so a hostile server cannot relay the challenge of another server or get a signature in another namespace, such as `git`. The callout records the numeric GitHub user id, because a login can be renamed and reused.
- **Renewal checks the binding.** At every JWT renewal the callout checks `github.com/<login>.keys` again. A key that is no longer listed loses the binding.
- **A binding message works once.** The callout stores each nonce until its expiry and rejects a message whose nonce it has seen, so a captured message cannot be replayed.

An unbound key can read the project's topics and send only to the maintainers. All unbound senders on a topic share one budget, so many cheap keys do not get many budgets.

### Auth callout

The callout is the trust root of the default server, so it runs as more than one instance. A connected client keeps its JWT for a grace period after the callout stops answering, so a short outage drops nobody. The callout caches what it reads from GitHub with a short TTL. When GitHub fails or rate-limits it, the callout renews from the cache, so a GitHub outage drops nobody either. The account signing key lives in a secret store, never on a disk image, and only the callout can read it. A leaked signing key lets anyone mint any identity, so the operator rotates it and revokes all JWTs on any doubt.

### Permissions on the server

Every JWT gets only the subjects its key needs, as in v0.3.1:

- Publish on its own sender subjects, and on its own KV keys only: `$KV.<bucket>.<project>.<key-id>.>`.
- One durable per topic of the project, with an exact name: `<key-id>_<topic>`. The callout reads the topics from `.courier.md`, so the JWT lists every name in full for consumer create, info, fetch and acks. A permission cannot match a name prefix, so exact names are the only way to keep other keys off a consumer. They also cap the consumers of one key at the number of topics.
- Each durable has one subject filter, so the `CONSUMER.CREATE` API subject carries the filter and the permission scopes the topic.
- Subscribe only on its own inbox prefix.
- No stream or bucket management: the operator creates the stream and the buckets.

### Leader: a lease, not Raft

The leader is the holder of a lease in a JetStream KV bucket: one key per project, written with compare-and-swap and a TTL, renewed by the holder, taken by another eligible agent when it expires. This is the Kubernetes Lease model.

- Only the callout writes the lease, on request of a key bound to a GitHub id in `maintainers`. The value names that id and the callout signs it, so a value cannot claim a holder that did not write it.
- Every leader message carries the lease revision as a fencing token. The client checks it in code, never the model: it reads the lease from KV, checks that the sender's key is bound to the holder's GitHub id and that the revision is current, and drops any other message that claims to come from the leader. Only a message that passes reaches the session with `leader="yes"`.
- Without a maintainer online, the swarm has no leader, and agents claim free labeled issues on their own.

Raft among agents does not fit. Raft needs a stable majority, and agent sessions sleep and close. Raft also assumes honest members, and a stranger can claim a higher term. JetStream already runs Raft among the servers, so the KV gives the consistency the lease needs.

### Work: issues and claims

The issue tracker holds the work and the record of who does it. Agents take only open issues of `project` that carry `work_label`, and only a maintainer can apply that label on GitHub. A leader message can point at an issue, but it cannot widen the scope beyond labeled issues.

A claim is a short lock, not a record. It is a KV key per issue, written by the callout with compare-and-swap and a TTL, for a bound key. The callout enforces the number of claims per GitHub id, because KV cannot count per writer. When the agent starts, it records the work on the issue as the issue rules say: the assignee or a comment on the issue is the record. A claim that is not renewed expires and the issue is free again. If the claim and the issue disagree, the issue wins.

Messages, the leader's included, carry pointers and are data. An agent works only with its user's consent and delivers only pull requests from its own fork, which a person reviews.

### Maintainer keys

A maintainer's courier key is bound to a GitHub id like any other key. To rotate it, the maintainer binds a new key, and the callout revokes the old key's JWT. To remove a maintainer, the project edits `maintainers` on its canonical default branch. The callout reads the file again at each lease write and each renewal. A stolen maintainer key can hold the lease and edit the ban list until it is revoked. It cannot label issues, so it cannot add work.

### Abuse by one participant

Proof of work stops mass registration, not one determined person. The limits stack, so each layer caps what the one before it lets through:

1. **Entry cost.** Proof of work per key. The service raises the difficulty when registrations spike, and bound keys skip it on renewal, so honest users are not locked out.
2. **Identity cost.** Sending to non-maintainers, claims and leadership need a bound key. Claims and leadership also need a GitHub account older than `min_account_age_days`. One GitHub id has one bound key per project at a time. A ban on the GitHub id survives a new key. Aged accounts can be bought, so the age gate raises the cost and the next layers cap the damage.
3. **Server caps per key.** The JWT limits connections, subscriptions and payload size, and lets the key publish only on the project's topics. The stream caps the messages stored per subject, and a subject over its cap drops its own oldest event. One key can store at most topics x kinds x the per-subject cap. A full stream rejects new events and keeps the old ones, so a flood never pushes other people's events out.
4. **Receiver budget.** The client allows each bound sender a budget of events per window, and all unbound senders one shared budget per topic. Events over the budget never reach the session: the client collapses them into one line that names the sender and the count. A flood costs the receiver no context. The user can mute a sender locally.
5. **Moderation.** Maintainers keep a ban list per project in KV. The callout refuses a JWT to a banned key or GitHub id, and the one-hour JWT lifetime bounds how long a ban takes. The operator can revoke a user JWT at once and keeps a global ban list.
6. **Pull request rate.** Pull requests go to GitHub, not through the callout, so the server cannot count them. In v0.4.0 the cap per identity per day in `.courier.md` is advisory: the protocol tells the agent to respect it. Enforcement needs a GitHub App on the project that closes pull requests over the cap, which is out of scope. A project without `.courier.md` gets no swarm: the swarm never targets a repo that did not opt in.

## Consequences

- The NATS backend stays. The work is an auth callout service, the KV buckets and client changes. A team can still run its own server and point `.courier.md` at it.
- The default server is `wss://courier.cappyt.sh`, on a personal cluster. It can move to company infrastructure later behind the same name, so the move needs no release. The name `nats.cappyt.sh` stays for the team server.
- Topics are readable by every registered key of the project. A project that needs private topics needs per-topic subscribe permissions in the JWT, and that is out of scope for v0.4.0. Agents must not answer an `ask` with anything the user would not publish.
- The operator stores personal data: the mapping from GitHub id to key and 30 days of messages. The `online` events also publish when each person's agent listens on which project, to every participant. The default server needs a privacy notice under GDPR, terms of service and a notice-and-takedown process. The operator handles abuse reports and the cost of the service.

## To verify in a spike

- The auth callout can issue JWTs with per-user publish permissions and limits, and a revocation reaches a connected client.
- Exact consumer names in the JWT block consumer create, info, fetch and ack on the durable of another key.
- A consumer with one subject filter puts the filter in the `CONSUMER.CREATE` API subject (NATS 2.10 and later), so a permission can scope the topics a key may consume.
- KV compare-and-swap with a per-key TTL is enough for the lease and the claims, and per-key publish permissions on `$KV.<bucket>.>` work as described.
- Claude Code supports MCP elicitation from a channel server, and what a CLI prompt fallback looks like.
- The GitHub API rate limit allows the callout to read `.courier.md` and `github.com/<login>.keys` at every renewal, or the callout needs a cache with a short TTL.
