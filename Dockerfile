FROM --platform=$BUILDPLATFORM rust:1-bookworm AS builder
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
COPY . .
RUN --mount=type=cache,id=${CARGO_CACHE_SCOPE}-cargo-home,target=/var/cache/layer-cargo \
    --mount=type=cache,id=${CARGO_CACHE_SCOPE}-rust-target-${TARGETARCH},target=/app/target case "$TARGETARCH" in \
      amd64) target=x86_64-unknown-linux-gnu ;; \
      arm64) target=aarch64-unknown-linux-gnu; export PKG_CONFIG_LIBDIR=/usr/lib/aarch64-linux-gnu/pkgconfig ;; \
      *) echo "unsupported architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    cargo build --locked --release --target "$target" -p hevlayer-gateway --bin hevlayer-gateway --features pgvector \
    && mkdir /out && cp /app/target/"$target"/release/hevlayer-gateway /out/

FROM debian:bookworm-slim
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*
COPY --from=builder /out/hevlayer-gateway /usr/local/bin/hevlayer-gateway
LABEL com.hevlayer.edition=ce
EXPOSE 8080
CMD ["hevlayer-gateway"]
