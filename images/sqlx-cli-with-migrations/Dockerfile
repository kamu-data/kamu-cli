ARG IMAGE_SQLX_TAG
FROM ghcr.io/kamu-data/sqlx-cli:$IMAGE_SQLX_TAG

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

COPY scripts /scripts

COPY ./migrations/ /migrations/
