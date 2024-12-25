# Base image info: https://jupyter-docker-stacks.readthedocs.io/en/latest/using/selecting.html
# Base image tags: https://quay.io/repository/jupyter/minimal-notebook
# Customization is based on: https://github.com/jupyter-incubator/sparkmagic/blob/master/Dockerfile.jupyter
FROM quay.io/jupyter/minimal-notebook:2024-12-09
ARG TARGETPLATFORM


#########################################################################################
USER root

RUN apt update && \
    apt -y install netcat-traditional curl wget gnupg unzip jq && \
    apt-get clean && rm -rf /var/lib/apt/lists /var/cache/apt/archives

COPY requirements/$TARGETPLATFORM/env.yaml env.yaml

RUN mamba env update -y -f env.yaml && \
    mamba clean --all -f -y && \
    rm env.yaml && \
    fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"


#########################################################################################
USER $NB_USER

COPY overrides.json /opt/conda/share/jupyter/lab/settings/overrides.json

RUN jupyter labextension disable "@jupyterlab/apputils-extension:announcements"

#########################################################################################
USER root
RUN fix-permissions "/home/${NB_USER}"

USER $NB_USER

CMD ["jupyter", "lab", "--ip", "0.0.0.0", "--port", "8080", "--NotebookApp.iopub_data_rate_limit=1e10"]
