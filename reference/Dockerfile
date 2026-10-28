FROM rust:1-bookworm

RUN apt-get update \
    && apt-get install -y libzmq3-dev pkg-config protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml build.rs ./
COPY proto ./proto
COPY src ./src

RUN cargo build --release

CMD ["./target/release/reference"]