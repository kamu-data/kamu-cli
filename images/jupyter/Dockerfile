# Base image info: https://jupyter-docker-stacks.readthedocs.io/en/latest/using/selecting.html
# Base image tags: https://quay.io/repository/jupyter/minimal-notebook
# Customization is based on: https://github.com/jupyter-incubator/sparkmagic/blob/master/Dockerfile.jupyter
FROM quay.io/jupyter/minimal-notebook:2024-02-13
ARG TARGETPLATFORM


#########################################################################################
USER root

RUN apt update && \
    apt -y install curl wget gnupg unzip jq && \
    apt-get clean && rm -rf /var/lib/apt/lists /var/cache/apt/archives

COPY requirements/$TARGETPLATFORM/requirements.txt requirements.txt

RUN mamba install -y --file requirements.txt && \
    mamba clean --all -f -y &&  \
    rm requirements.txt &&  \
    fix-permissions "${CONDA_DIR}" &&  \
    fix-permissions "/home/${NB_USER}"


#########################################################################################
USER $NB_USER

COPY kamu.py /opt/conda/lib/python3.11/site-packages/kamu.py
COPY sparkmagic.json /home/$NB_USER/.sparkmagic/config.json

RUN jupyter nbextension enable --py --sys-prefix widgetsnbextension
RUN jupyter-kernelspec install --user $(pip show sparkmagic | grep Location | cut -d" " -f2)/sparkmagic/kernels/sparkkernel
RUN jupyter-kernelspec install --user $(pip show sparkmagic | grep Location | cut -d" " -f2)/sparkmagic/kernels/pysparkkernel
RUN jupyter-kernelspec install --user $(pip show sparkmagic | grep Location | cut -d" " -f2)/sparkmagic/kernels/sparkrkernel
RUN jupyter serverextension enable --py sparkmagic


#########################################################################################
USER root
RUN fix-permissions "/home/${NB_USER}"

USER $NB_USER

# TODO: Remove show_banner option after Sparkmagic supports novebook >= 7.0.0
# See: https://github.com/jupyter-incubator/sparkmagic/issues/885
CMD ["jupyter", "notebook", "--ip", "0.0.0.0", "--port", "8080", "--NotebookApp.show_banner=False"]
