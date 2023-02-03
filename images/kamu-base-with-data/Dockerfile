ARG KAMU_VERSION

FROM kamudata/kamu-base:$KAMU_VERSION

COPY datasets /tmp/datasets

# Init datasets and clean up stuff to keep the image light
RUN kamu init && \
    /tmp/datasets/init.sh && \
    rm -rf /tmp/datasets && \
    podman images -q | xargs podman rmi
