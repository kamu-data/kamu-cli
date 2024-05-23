FROM rust:1.78-alpine as builder

ARG VERSION

LABEL org.opencontainers.image.source https://github.com/kamu-data/kamu-cli

################################################################################

# cargo-binstall: preparation
RUN apk add --no-cache \
    curl \
    bash

# cargo-binstall: install
RUN curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash

# sqlx: install
RUN cargo binstall sqlx-cli@$VERSION -y

################################################################################

FROM alpine:latest

COPY --from=builder /usr/local/cargo/bin/sqlx /usr/bin/sqlx

ENTRYPOINT ["/usr/bin/sqlx"]
