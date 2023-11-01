# Based on: https://github.com/jupyter-incubator/sparkmagic/blob/master/Dockerfile.jupyter
# Tags: https://hub.docker.com/r/jupyter/base-notebook/tags
FROM docker.io/jupyter/base-notebook:2022-11-15

USER root

RUN apt update \
    && apt -y install curl wget gnupg unzip jq \
    && rm -rf /var/lib/apt/lists/*


#########################################################################################
USER $NB_USER

# This is needed because requests-kerberos fails to install on debian due to missing linux headers
RUN conda install requests-kerberos==0.12.0 -y

COPY requirements.txt requirements.txt
RUN pip --no-cache-dir install --upgrade -r requirements.txt && rm requirements.txt

COPY kamu.py /opt/conda/lib/python3.10/site-packages/kamu.py

COPY sparkmagic.json /home/$NB_USER/.sparkmagic/config.json
RUN jupyter nbextension enable --py --sys-prefix widgetsnbextension
RUN jupyter-kernelspec install --user $(pip show sparkmagic | grep Location | cut -d" " -f2)/sparkmagic/kernels/sparkkernel
RUN jupyter-kernelspec install --user $(pip show sparkmagic | grep Location | cut -d" " -f2)/sparkmagic/kernels/pysparkkernel
RUN jupyter-kernelspec install --user $(pip show sparkmagic | grep Location | cut -d" " -f2)/sparkmagic/kernels/sparkrkernel
RUN jupyter serverextension enable --py sparkmagic


#########################################################################################
USER root
RUN chown -R $NB_USER:users /home/$NB_USER

USER $NB_USER
CMD ["jupyter", "notebook", "--ip", "0.0.0.0", "--port", "80", "--allow-root"]
