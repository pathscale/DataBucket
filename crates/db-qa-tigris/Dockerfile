FROM rust:1.94-bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends musl-tools \
    && rm -rf /var/lib/apt/lists/* \
    && rustup target add x86_64-unknown-linux-musl
WORKDIR /build
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/crates/db-qa-tigris/target \
    cargo build --release --bin db-qa-tigris --target x86_64-unknown-linux-musl \
        --manifest-path crates/db-qa-tigris/Cargo.toml \
    && cp crates/db-qa-tigris/target/x86_64-unknown-linux-musl/release/db-qa-tigris /db-qa-tigris

FROM scratch
COPY --from=builder /db-qa-tigris /db-qa-tigris
ENTRYPOINT ["/db-qa-tigris"]
