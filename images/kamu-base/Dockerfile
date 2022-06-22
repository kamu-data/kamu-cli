FROM ubuntu:focal

ARG KAMU_VERSION

# Podman
# Source: https://github.com/containers/podman/blob/056f492f59c333d521ebbbe186abde0278e815db/contrib/podmanimage/stable/Dockerfile
RUN apt update && \
    apt -y install curl wget gnupg unzip jq && \
    . /etc/os-release && \
    echo "deb https://download.opensuse.org/repositories/devel:/kubic:/libcontainers:/stable/xUbuntu_${VERSION_ID}/ /" \
        | tee /etc/apt/sources.list.d/devel:kubic:libcontainers:stable.list && \
    curl -L "https://download.opensuse.org/repositories/devel:/kubic:/libcontainers:/stable/xUbuntu_${VERSION_ID}/Release.key" \
        | apt-key add - && \
    apt update && \
    apt -y install podman fuse-overlayfs && \
    rm -rf /var/lib/apt/lists/*

COPY podman/containers.conf /etc/containers/containers.conf
COPY podman/storage.conf /etc/containers/storage.conf
COPY podman/containers-user.conf /root/.config/containers/containers.conf
COPY podman/storage-user.conf /root/.config/containers/storage.conf

# Create empty storage not to get errors when it's not mounted 
# See: https://www.redhat.com/sysadmin/image-stores-podman
RUN mkdir -p \
    /var/lib/containers/shared/overlay-images \ 
    /var/lib/containers/shared/overlay-layers \
    /var/lib/containers/shared/vfs-images \
    /var/lib/containers/shared/vfs-layers && \
    touch /var/lib/containers/shared/overlay-images/images.lock && \
    touch /var/lib/containers/shared/overlay-layers/layers.lock && \
    touch /var/lib/containers/shared/vfs-images/images.lock && \
    touch /var/lib/containers/shared/vfs-layers/layers.lock


# AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf aws*


# Tini
ADD https://github.com/krallin/tini/releases/download/v0.19.0/tini /usr/bin/tini
RUN chmod +x /usr/bin/tini


# Kamu
RUN wget -q https://github.com/kamu-data/kamu-cli/releases/download/v$KAMU_VERSION/kamu-cli-x86_64-unknown-linux-gnu.tar.gz && \
    tar -xf kamu-cli-x86_64-unknown-linux-gnu.tar.gz && \
    chmod +x kamu-cli-x86_64-unknown-linux-gnu/kamu && \
    mv kamu-cli-x86_64-unknown-linux-gnu/kamu /usr/local/bin/ && \
    rm -rf kamu-cli-x86_64-unknown-linux-gnu* && \
    echo "source <(kamu completions bash)" >> /root/.bashrc
COPY .kamuconfig /.kamuconfig


RUN mkdir -p /opt/kamu/workspace
WORKDIR /opt/kamu/workspace
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["bash"]
