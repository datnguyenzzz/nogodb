# Lightweight Rust Slim image using modern Rust compiler supporting edition 2024
FROM rust:1.85-slim

WORKDIR /usr/src/query_engine

# Copy the entire workspace into the container
COPY . .

# Run cargo test directly on container startup!
CMD ["cargo", "test"]