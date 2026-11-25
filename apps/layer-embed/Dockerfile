# Build context: apps/layer-embed. Build-only Oven validation is a later gate.
FROM --platform=$BUILDPLATFORM python:3.12-slim-bookworm@sha256:392307d22300de8b5986851a12d9176dfc0fc073e65bf6523ebd7dcbeb23564e AS artifacts
WORKDIR /package
COPY artifacts.lock.json manifest.json ./
COPY licenses/ licenses/
COPY scripts/prepare-artifacts.py scripts/prepare-artifacts.py
RUN python scripts/prepare-artifacts.py /models

FROM --platform=$BUILDPLATFORM rust:1.98-bookworm@sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e AS builder
ARG TARGETARCH
# Rust and build scripts execute on the build host, including for arm64.
# Native builds can fetch dependencies while the ARM toolchain is prepared.
RUN if [ "$TARGETARCH" = arm64 ]; then dpkg --add-architecture arm64; fi \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
       pkg-config libssl-dev protobuf-compiler libprotobuf-dev ca-certificates git \
    && if [ "$TARGETARCH" = arm64 ]; then \
         apt-get install -y --no-install-recommends \
           gcc-aarch64-linux-gnu g++-aarch64-linux-gnu libssl-dev:arm64 \
         && rustup target add aarch64-unknown-linux-gnu; \
       fi \
    && rm -rf /var/lib/apt/lists/*
ARG CARGO_BUILD_JOBS=8
ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
    CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc \
    CXX_aarch64_unknown_linux_gnu=aarch64-linux-gnu-g++ \
    PKG_CONFIG_ALLOW_CROSS=1 CARGO_BUILD_JOBS=$CARGO_BUILD_JOBS \
    CARGO_NET_GIT_FETCH_WITH_CLI=true \
    CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_RUSTFLAGS="-C target-feature=+fp16"
# Keep Cargo's package-cache lock beside the shared registry and Git data.
ARG CARGO_CACHE_SCOPE=layer
ENV CARGO_HOME=/var/cache/layer-cargo
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
# gemm 0.19 ARM half kernels require assembler FP16 support, as in the repo.
RUN --mount=type=cache,id=${CARGO_CACHE_SCOPE}-cargo-home,target=/var/cache/layer-cargo \
    --mount=type=cache,id=${CARGO_CACHE_SCOPE}-rust-target-${TARGETARCH},target=/app/target case "$TARGETARCH" in \
      amd64) target=x86_64-unknown-linux-gnu ;; \
      arm64) target=aarch64-unknown-linux-gnu; export PKG_CONFIG_LIBDIR=/usr/lib/aarch64-linux-gnu/pkgconfig ;; \
      *) echo "unsupported architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    if [ "$TARGETARCH" = arm64 ]; then export RUSTFLAGS='-C target-feature=+fp16'; fi; \
    cargo build --release --locked --target "$target" \
    && mkdir /out && cp /app/target/"$target"/release/layer-embed /out/

FROM debian:bookworm-slim@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251
# No package manager, Python, HF client, curl, credentials or download entrypoint.
RUN rm -rf /usr/bin/apt* /usr/bin/dpkg* /usr/sbin/dpkg* /var/lib/apt /var/lib/dpkg
COPY --from=builder /out/layer-embed /usr/local/bin/layer-embed
COPY --from=artifacts /models/ /opt/layer-embed/models/
USER 65532:65532
EXPOSE 8081
ENTRYPOINT ["/usr/local/bin/layer-embed"]
