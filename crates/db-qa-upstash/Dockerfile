FROM rust:1.94-bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends musl-tools \
    && rm -rf /var/lib/apt/lists/* \
    && rustup target add x86_64-unknown-linux-musl
WORKDIR /build
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/crates/db-qa-upstash/target \
    cargo build --release --target x86_64-unknown-linux-musl \
        --manifest-path crates/db-qa-upstash/Cargo.toml \
    && cp crates/db-qa-upstash/target/x86_64-unknown-linux-musl/release/db-qa-upstash /db-qa-upstash

FROM scratch
COPY --from=builder /db-qa-upstash /db-qa-upstash
ENTRYPOINT ["/db-qa-upstash"]
