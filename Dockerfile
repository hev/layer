FROM rust:1-bookworm AS builder
WORKDIR /app
COPY . .
RUN cargo build --release -p hevlayer-gateway --bin hevlayer-gateway

FROM debian:bookworm-slim
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/hevlayer-gateway /usr/local/bin/hevlayer-gateway
EXPOSE 8080
CMD ["hevlayer-gateway"]
