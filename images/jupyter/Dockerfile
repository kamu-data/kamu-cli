# Base image info: https://jupyter-docker-stacks.readthedocs.io/en/latest/using/selecting.html
# Base image tags: https://quay.io/repository/jupyter/minimal-notebook
# Customization is based on: https://github.com/jupyter-incubator/sparkmagic/blob/master/Dockerfile.jupyter
FROM quay.io/jupyter/minimal-notebook:2024-12-09
ARG TARGETPLATFORM


#########################################################################################
USER root

RUN apt update && \
    apt -y install curl wget gnupg unzip jq && \
    apt-get clean && rm -rf /var/lib/apt/lists /var/cache/apt/archives

COPY requirements/$TARGETPLATFORM/env.yaml env.yaml

# TODO: Semi-permanent hack for `mapboxgl` package being broken in conda-forge
# See: https://github.com/kamu-data/kamu-cli/issues/533
RUN mamba env update -y -f env.yaml && \
    pip install --no-cache-dir mapboxgl && \
    mamba clean --all -f -y &&  \
    rm env.yaml &&  \
    fix-permissions "${CONDA_DIR}" &&  \
    fix-permissions "/home/${NB_USER}"


#########################################################################################
USER $NB_USER

COPY overrides.json /opt/conda/share/jupyter/lab/settings/overrides.json
COPY kamu.py /opt/conda/lib/python3.12/site-packages/kamu.py
COPY sparkmagic.json /home/$NB_USER/.sparkmagic/config.json

RUN jupyter-kernelspec install --user $(pip show sparkmagic | grep Location | cut -d" " -f2)/sparkmagic/kernels/sparkkernel && \
    jupyter-kernelspec install --user $(pip show sparkmagic | grep Location | cut -d" " -f2)/sparkmagic/kernels/pysparkkernel && \
    jupyter-kernelspec install --user $(pip show sparkmagic | grep Location | cut -d" " -f2)/sparkmagic/kernels/sparkrkernel && \
    jupyter server extension enable --py sparkmagic && \
    jupyter labextension disable "@jupyterlab/apputils-extension:announcements"


#########################################################################################
USER root
RUN fix-permissions "/home/${NB_USER}"

USER $NB_USER

CMD ["jupyter", "lab", "--ip", "0.0.0.0", "--port", "8080"]
