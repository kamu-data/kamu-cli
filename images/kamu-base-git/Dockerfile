FROM ubuntu:focal AS builder

ARG KAMU_BRANCH=master


WORKDIR /app/

ENV PATH=/root/.cargo/bin:$PATH

RUN apt update  \
    && apt install -y  \
      git  \
      curl \
      gcc \
    && git clone https://github.com/kamu-data/kamu-cli  \
      --branch $KAMU_BRANCH  \
      --depth 1 \
      /app/ \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs  \
      | sh -s -- --default-toolchain nightly -y \
    && cargo build -p kamu-cli

################################################################################

FROM ghcr.io/kamu-data/kamu-base:latest

COPY --from=builder /app/target/debug/kamu-cli /usr/bin/kamu
