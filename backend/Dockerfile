FROM rust:1-bookworm AS builder
# build.rs compiles the proto of chat's grpc client
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src
RUN cargo build --release --locked

# glibc and libgcc for the binary, no shell or package manager
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /app/target/release/holidayfriend-server /server
EXPOSE 8080
ENTRYPOINT ["/server"]
