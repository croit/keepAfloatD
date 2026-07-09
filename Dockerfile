# Base-image registry. Defaults to `docker.io` so a local `docker build` works unchanged; override
# with `--build-arg DOCKER_HUB_MIRROR=<mirror>` to pull base images through a registry mirror and
# avoid Docker Hub's unauthenticated pull rate limit (HTTP 429).
ARG DOCKER_HUB_MIRROR=docker.io

FROM ${DOCKER_HUB_MIRROR}/library/rust:1.91.1-slim-trixie AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        binutils \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml Cargo.lock ./

RUN mkdir src \
    && printf 'fn main() {}\n' > src/main.rs \
    && cargo build --release --locked \
    && rm -rf src target/release/.fingerprint/keepafloatd* target/release/deps/keepafloatd* target/release/keepafloatd

COPY src ./src

RUN cargo build --release --locked \
    && strip target/release/keepafloatd

FROM builder AS dev

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        libssl-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/* \
    && rustup component add rustfmt clippy \
    && cargo install cargo-tarpaulin --locked

CMD ["cargo", "test", "--all-targets", "--locked"]

# Re-declare the global ARG so this stage's FROM can reference it (Docker scopes pre-FROM ARGs).
ARG DOCKER_HUB_MIRROR=docker.io
FROM ${DOCKER_HUB_MIRROR}/library/debian:trixie-slim AS runtime-base

LABEL org.opencontainers.image.title="keepafloatd" \
      org.opencontainers.image.description="Raft-based VIP failover daemon" \
      org.opencontainers.image.source="https://github.com/croit/keepAfloatD" \
      org.opencontainers.image.licenses="AGPL-3.0-only" \
      org.opencontainers.image.vendor="croit GmbH" \
      io.croit.keepafloatd.license-alternative="Commercial license available from croit.io" \
      io.croit.keepafloatd.default-config="/etc/keepafloatd/config.yaml" \
      io.croit.keepafloatd.required-capabilities="CAP_NET_ADMIN" \
      io.croit.keepafloatd.optional-capabilities="CAP_NET_RAW" \
      io.croit.keepafloatd.readonly-rootfs="recommended"

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        iproute2 \
        iputils-arping \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /etc/keepafloatd /var/lib/keepafloatd /usr/share/doc/keepafloatd/licenses \
    && echo 'keepafloatd:x:10001:' >> /etc/group \
    && echo 'keepafloatd:x:10001:10001:keepafloatd:/var/lib/keepafloatd:/bin/sh' >> /etc/passwd \
    && chown -R 10001:10001 /etc/keepafloatd /var/lib/keepafloatd

COPY LICENSE README.md THIRD_PARTY_LICENSES.md /usr/share/doc/keepafloatd/
COPY LICENSES/ /usr/share/doc/keepafloatd/licenses/

ENV HOME=/var/lib/keepafloatd
WORKDIR /var/lib/keepafloatd

HEALTHCHECK NONE

ENTRYPOINT ["/usr/local/bin/keepafloatd"]
CMD ["-c", "/etc/keepafloatd/config.yaml"]

# Release runtime: built from the pre-cross-compiled binary in ./dist instead of recompiling in the
# image, so an arm64 image does not run the whole Rust build under QEMU emulation. `docker buildx`
# sets TARGETARCH per target platform, selecting the matching binary the release build already made.
# The release job selects this explicitly with `--target runtime-dist`.
FROM runtime-base AS runtime-dist
ARG TARGETARCH
COPY dist/keepafloatd-linux-${TARGETARCH} /usr/local/bin/keepafloatd
RUN chmod 0755 /usr/local/bin/keepafloatd
USER 10001:10001

# CI / e2e runtime: binary compiled in-image from the `builder` stage (these builds have no
# pre-built binary to hand in). USER follows the copy so the root-owned binary lands correctly.
# Kept LAST so a bare `docker build .` (no --target, e.g. the e2e job) defaults to this
# self-contained stage rather than runtime-dist, which requires ./dist.
FROM runtime-base AS runtime
COPY --from=builder /app/target/release/keepafloatd /usr/local/bin/keepafloatd
USER 10001:10001
