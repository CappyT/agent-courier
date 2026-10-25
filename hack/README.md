# Development container

Every Go and Bun command runs in the distrobox container `agent-courier-dev`. The file `hack/distrobox.ini` defines it. Use it to get the same tools on every machine. The `go` on the host PATH is a wrapper for another container. Do not use it.

## Pinned versions

`hack/install-tools.sh` installs these tools. Each download is checked with `sha256sum` before install, then moved into place with `mv`. A failed check stops the install and leaves no partial tool.

| Tool | Version | sha256 of the download |
| --- | --- | --- |
| Go | 1.27.3 | 65efbc80fdc1fa2cd2631cad5b481777a68e8725f34e89f50fc6ac714e379b40 |
| Bun | 1.4.3 | 3e730528d89775f03f87f05c06787c4df4099d0a37f3f252e7b829cb23816e18 |
| Base image | docker.io/library/ubuntu:24.04 | |

The sha256 values come from the upstream checksum files. Bun runs the tests of the OpenCode plugin in `opencode/`. The container also has make, git, curl, jq, and gcc for `go test -race`, which needs cgo. The tools are for amd64.

To change a version, edit `hack/install-tools.sh` and this table together. Then remove and create the container again.

## Go environment

`/etc/profile.d/go.sh` puts `/usr/local/go/bin`, `/usr/local/bin` and `$HOME/go/bin` first on PATH. They shadow the `go` wrapper and any `bun` in the host `~/.local/bin`, which the container shares.

The container uses its own Go module cache, `$HOME/.cache/agent-courier/gomod` (`GOMODCACHE`). The shared `~/go/pkg/mod` has files owned by other users.

## Create

Run it from the repo root. The init hook runs `hack/install-tools.sh` from the current directory.

```sh
distrobox assemble create --file hack/distrobox.ini </dev/null
```

The first start downloads the tools. It takes a few minutes.

## Enter

Run one command:

```sh
distrobox enter agent-courier-dev -- bash -lc '<cmd>' </dev/null
```

Open a shell:

```sh
distrobox enter agent-courier-dev
```

Agents run these commands with the sandbox disabled. Podman cannot start inside the sandbox. The `</dev/null` redirect stops the create prompt.

## Remove

```sh
distrobox rm agent-courier-dev --force </dev/null
```

Use the exact name. Do not remove other containers.
