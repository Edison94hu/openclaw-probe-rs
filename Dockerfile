FROM rust:1.82-slim AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock* ./
COPY src/ ./src/
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /build/target/release/openclaw-probe .
COPY config.json .
RUN mkdir -p data

EXPOSE 8000
CMD ["./openclaw-probe"]
