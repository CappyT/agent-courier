#!/bin/sh
# Installs the pinned tools of agent-courier-dev. Called by the init hooks in hack/distrobox.ini as root.
# Each tool is downloaded to a temp dir, checked with sha256sum, then moved into place with mv.
# The [ -x ] guard then never sees a half-installed tool. amd64 only.
set -eu

GO_VERSION=1.27.3
GO_SHA256=65efbc80fdc1fa2cd2631cad5b481777a68e8725f34e89f50fc6ac714e379b40
BUN_VERSION=1.4.3
BUN_SHA256=3e730528d89775f03f87f05c06787c4df4099d0a37f3f252e7b829cb23816e18

T=$(mktemp -d /var/tmp/ac-tools.XXXXXX)
trap 'rm -rf "$T"' EXIT

# fetch <url> <sha256> <name>: downloads to $T/<name> and verifies it.
fetch() {
	curl -fsSL --retry 3 -o "$T/$3" "$1"
	echo "$2  $T/$3" | sha256sum -c - >/dev/null
}

if [ ! -x /usr/local/go/bin/go ]; then
	fetch "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" "$GO_SHA256" go.tgz
	mkdir "$T/go.x"
	tar -C "$T/go.x" -xzf "$T/go.tgz"
	rm -rf /usr/local/go
	mv "$T/go.x/go" /usr/local/go
fi

if [ ! -x /usr/local/bin/bun ]; then
	fetch "https://github.com/oven-sh/bun/releases/download/bun-v${BUN_VERSION}/bun-linux-x64.zip" "$BUN_SHA256" bun.zip
	unzip -q "$T/bun.zip" -d "$T/bun.x"
	chmod +x "$T/bun.x/bun-linux-x64/bun"
	mv "$T/bun.x/bun-linux-x64/bun" /usr/local/bin/bun
fi
