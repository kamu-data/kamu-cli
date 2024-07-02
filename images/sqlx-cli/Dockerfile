################################################################################
# Builder
################################################################################
FROM rust:1.78-alpine as builder

ARG VERSION

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
# Image
################################################################################

FROM alpine:latest

LABEL org.opencontainers.image.source https://github.com/kamu-data/kamu-cli

################################################################################

# Install dependencies
RUN apk --no-cache add \
    bash \
    curl \
    jq && \
    # Download and install the AWS CLI version 2
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip aws

COPY --from=builder /usr/local/cargo/bin/sqlx /usr/bin/sqlx

COPY scripts /scripts

ENTRYPOINT ["/usr/bin/sqlx"]
