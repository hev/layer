# Build context: apps/layer-embed. Build-only Depot validation is a later gate.
FROM python:3.12-slim-bookworm@sha256:392307d22300de8b5986851a12d9176dfc0fc073e65bf6523ebd7dcbeb23564e AS artifacts
WORKDIR /package
COPY artifacts.lock.json manifest.json ./
COPY licenses/ licenses/
COPY scripts/prepare-artifacts.py scripts/prepare-artifacts.py
RUN python scripts/prepare-artifacts.py /models

FROM rust:1.98-bookworm@sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
# gemm 0.19 ARM half kernels require assembler FP16 support, as in the repo.
RUN case "$(uname -m)" in aarch64) export RUSTFLAGS='-C target-feature=+fp16';; esac; cargo build --release --locked

FROM debian:bookworm-slim@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251
# No package manager, Python, HF client, curl, credentials or download entrypoint.
RUN rm -rf /usr/bin/apt* /usr/bin/dpkg* /usr/sbin/dpkg* /var/lib/apt /var/lib/dpkg
COPY --from=builder /build/target/release/layer-embed /usr/local/bin/layer-embed
COPY --from=artifacts /models/ /opt/layer-embed/models/
USER 65532:65532
EXPOSE 8081
ENTRYPOINT ["/usr/local/bin/layer-embed"]
