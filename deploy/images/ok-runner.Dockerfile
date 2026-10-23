# The ok-runner image, for workers and the deployer, which check commits out and run Bazel: the ok
# binary, ok's jj (the okfs daemon), bazelisk (which runs the Bazel a repository's .bazelversion
# names), bubblewrap (the sandbox of what a run job runs), FUSE's fusermount3, Cloud Hypervisor,
# virtiofsd, and the guests' kernel (runs in microVMs, worker.microvm), and a C and C++ compiler,
# which Bazel's default C++ toolchain needs wherever a build reaches one. Build from the
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
# Cloud Hypervisor v54.0 and its guest kernel, for runs in microVMs (worker.microvm; virtiofsd is
# Debian's).
ADD --checksum=sha256:4f4f98a3d661a40f3e5272627cc895a1b2e525f2f6bedf5f895f9cd21ba5c65b \
    https://github.com/cloud-hypervisor/cloud-hypervisor/releases/download/v54.0/cloud-hypervisor-static /tmp/ch-amd64
ADD --checksum=sha256:e8a6d9786ea678e739f1a386682846e93742dcb9c7b6e3e0c62e3bda6cff0ef3 \
    https://github.com/cloud-hypervisor/cloud-hypervisor/releases/download/v54.0/cloud-hypervisor-static-aarch64 /tmp/ch-arm64
ADD --checksum=sha256:40f5ac32a703528a850dd2cedb3de5cb5bfc383c5abbff1424a135d7a85bec35 \
    https://github.com/cloud-hypervisor/linux/releases/download/ch-release-v7.2.8-20261009/bzImage-x86_64 /tmp/kernel-amd64
ADD --checksum=sha256:8f8f3d5a983809f87a4e1e9a13053a3361fda47ef04e09c771a41c5e037cb6c2 \
    https://github.com/cloud-hypervisor/linux/releases/download/ch-release-v7.2.8-20261009/Image-arm64 /tmp/kernel-arm64
RUN arch="${TARGETARCH:-$(dpkg --print-architecture)}" \
 && install -m 0755 "/tmp/bazelisk-${arch}" /usr/local/bin/bazelisk && ln -s bazelisk /usr/local/bin/bazel && rm /tmp/bazelisk-* \
 && install -m 0755 "/tmp/ch-${arch}" /usr/local/bin/cloud-hypervisor \
 && install -D -m 0644 "/tmp/kernel-${arch}" /usr/local/lib/ok/kernel && rm /tmp/ch-* /tmp/kernel-* \
 && apt-get update \
 && apt-get install -y --no-install-recommends bubblewrap ca-certificates fuse3 g++ git python3 util-linux virtiofsd zip unzip \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --create-home --uid 65532 ok
COPY --from=go /out/ok /usr/local/bin/ok
COPY --from=jj /out-jj /usr/local/bin/jj
# ok, by number: Kubernetes' runAsNonRoot verifies only a numeric user.
USER 65532:65532
EXPOSE 8472 8473
ENTRYPOINT ["/usr/local/bin/ok"]
