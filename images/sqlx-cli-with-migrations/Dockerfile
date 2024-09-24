ARG IMAGE_SQLX_TAG
FROM ghcr.io/kamu-data/sqlx-cli:$IMAGE_SQLX_TAG

LABEL org.opencontainers.image.source https://github.com/kamu-data/kamu-cli

################################################################################

# Install dependencies
RUN apk --no-cache add \
    bash \
    curl \
    jq \
    aws-cli \
    postgresql-client

COPY scripts /scripts
RUN chmod -R 0755 /scripts

COPY ./migrations/ /migrations/
