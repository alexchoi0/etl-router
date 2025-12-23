FROM rust:1.75-bookworm AS builder

WORKDIR /app

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# Build the operator
RUN cargo build --release -p conveyor-etl-operator

# Runtime image
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/conveyor-etl-operator /usr/local/bin/conveyor-etl-operator

ENTRYPOINT ["/usr/local/bin/conveyor-etl-operator"]
