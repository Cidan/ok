# The ok-runner image, for workers and the deployer, which check commits out and run Bazel: the ok
# binary, ok's jj (the okfs daemon), bazelisk (which runs the Bazel a repository's .bazelversion
# names), bubblewrap (the sandbox of what a run job runs), FUSE's fusermount3, and a C and C++
# compiler, which Bazel's default C++ toolchain needs wherever a build reaches one. Build from the
# repository's root:
#
#   docker build -f deploy/images/ok-runner.Dockerfile -t ok-runner .
FROM golang:1.26.9-trixie AS go
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/ok ./cmd/ok

FROM rust:1.98.1-trixie AS jj
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p ok-jj && cp target/release/jj /out-jj

FROM debian:trixie-slim
# Set by BuildKit and podman; the classic Docker builder leaves it empty, so the image's own
# architecture stands in for it.
ARG TARGETARCH
# bazelisk v1.29.0, as GitHub publishes it.
ADD --checksum=sha256:5a408715e932c0250d28bd84555f12edbf70117de42f9181691c736eacc4a992 \
    https://github.com/bazelbuild/bazelisk/releases/download/v1.29.0/bazelisk-linux-amd64 /tmp/bazelisk-amd64
ADD --checksum=sha256:e20e8b0f4f240091b7a55bf17b9398bd4f40ee70ae0208dff95dd4c445fb4010 \
    https://github.com/bazelbuild/bazelisk/releases/download/v1.29.0/bazelisk-linux-arm64 /tmp/bazelisk-arm64
RUN arch="${TARGETARCH:-$(dpkg --print-architecture)}" \
 && install -m 0755 "/tmp/bazelisk-${arch}" /usr/local/bin/bazelisk && ln -s bazelisk /usr/local/bin/bazel && rm /tmp/bazelisk-* \
 && apt-get update \
 && apt-get install -y --no-install-recommends bubblewrap ca-certificates fuse3 g++ git python3 util-linux zip unzip \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --create-home --uid 65532 ok
COPY --from=go /out/ok /usr/local/bin/ok
COPY --from=jj /out-jj /usr/local/bin/jj
# ok, by number: Kubernetes' runAsNonRoot verifies only a numeric user.
USER 65532:65532
EXPOSE 8472 8473
ENTRYPOINT ["/usr/local/bin/ok"]
