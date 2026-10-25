# The builder runs on the build platform and cross-compiles, so a
# multi-platform build needs no emulation.
FROM --platform=$BUILDPLATFORM docker.io/library/golang:1.27.3@sha256:f20d83a83e18dbd2a4714b8ffd4b30b287b6243962dd75608e86abcc63ba4efc AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags='-s -w' -o /out/agent-courier ./cmd/agent-courier

FROM gcr.io/distroless/static-debian13:nonroot@sha256:18f4cc26c9ffa7f20a359298be7deadac5c607c8e03a02b8119f1a5fc2f82c99
COPY --from=build /out/agent-courier /usr/local/bin/agent-courier
USER nonroot:nonroot
EXPOSE 4001/udp 4001/tcp
ENTRYPOINT ["/usr/local/bin/agent-courier"]
