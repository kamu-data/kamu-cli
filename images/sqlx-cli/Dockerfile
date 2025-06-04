################################################################################
# Builder
################################################################################

ARG RUSTC_VERSION

FROM rust:${RUSTC_VERSION}-alpine AS builder

ARG SQLX_VERSION

################################################################################

# sqlx: preparation
RUN apk add --no-cache \
    gcc \
    musl-dev

# sqlx: build
RUN cargo install "sqlx-cli@$SQLX_VERSION"  \
    --no-default-features \
    --features postgres,sqlite,mysql,rustls

################################################################################
# Image
################################################################################

FROM alpine:latest

LABEL org.opencontainers.image.source=https://github.com/kamu-data/kamu-cli

################################################################################

COPY --from=builder /usr/local/cargo/bin/sqlx /usr/bin/sqlx

ENTRYPOINT ["/usr/bin/sqlx"]
