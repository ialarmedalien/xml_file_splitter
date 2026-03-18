# syntax=docker/dockerfile:1

# Comments are provided throughout this file to help you get started.
# If you need more help, visit the Dockerfile reference guide at
# https://docs.docker.com/go/dockerfile-reference/

# Want to help us make this template better? Share your feedback here: https://forms.gle/ybq9Krt8jtBL3iCk7

ARG RUST_VERSION=1.93.1
ARG APP_NAME=xml_file_splitter

################################################################################
# Create a stage for building the application.

FROM rust:${RUST_VERSION} AS builder
ARG APP_NAME
WORKDIR /app

# Copy manifests first for layer caching
COPY Cargo.toml Cargo.lock ./

# Copy source and build
COPY src ./src
RUN cargo build --release

# Runtime stage
FROM debian:trixie-slim
ARG APP_NAME

# Install CA certificates and minimal dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends tini ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -r -s /bin/false nonroot

# Copy binary
COPY --from=builder /app/target/release/${APP_NAME} /usr/local/bin/

COPY --chmod=+x ./scripts/entrypoint.sh /app/
# Use the non-root user to run our application
USER nonroot
WORKDIR /app
ENTRYPOINT ["./entrypoint.sh"]
