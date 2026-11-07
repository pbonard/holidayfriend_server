FROM rust:1-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

# glibc and libgcc for the binary, no shell or package manager
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /app/target/release/holidayfriend-server /server
EXPOSE 8080
ENTRYPOINT ["/server"]
