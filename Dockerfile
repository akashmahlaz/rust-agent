# Stage 1: Build the Rust application
FROM rust:1.75-alpine AS builder

# Install build dependencies
RUN apk add --no-cache musl-dev pkgconfig openssl-dev openssl-libs-static

WORKDIR /app

# Copy manifests first for better caching
COPY Cargo.toml Cargo.lock* ./
COPY src ./src

# Build the application
RUN cargo build --release

# Stage 2: Create minimal runtime image
FROM alpine:3.19

# Install runtime dependencies
RUN apk add --no-cache ca-certificates libssl3

WORKDIR /app

# Copy the binary from builder
COPY --from=builder /app/target/release/server /usr/local/bin/

# Create data directories
RUN mkdir -p /data/workspaces /app/local_uploads

# Expose port
EXPOSE 8080

# Set environment
ENV RUST_LOG=info

# Run the server
CMD ["server"]
