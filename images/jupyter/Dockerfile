# Based on: https://github.com/jupyter-incubator/sparkmagic/blob/master/Dockerfile.jupyter
# TODO: Using old version due https://github.com/jupyter-incubator/sparkmagic/issues/492
FROM docker.io/jupyter/base-notebook:4a112c0f11eb

USER root

RUN apt update \
    && apt -y install curl wget gnupg unzip jq \
    && rm -rf /var/lib/apt/lists/*

# This is needed because requests-kerberos fails to install on debian due to missing linux headers
RUN conda install requests-kerberos -y

COPY kamu.py /opt/conda/lib/python3.8/site-packages/kamu.py


#########################################################################################
USER $NB_USER

RUN pip --no-cache-dir install --upgrade pip
RUN pip --no-cache-dir install --upgrade --ignore-installed setuptools
COPY requirements.in requirements.in
RUN pip --no-cache-dir install -r requirements.in && rm requirements.in

COPY sparkmagic.json /home/$NB_USER/.sparkmagic/config.json
RUN jupyter nbextension enable --py --sys-prefix widgetsnbextension
RUN jupyter-kernelspec install --user $(pip show sparkmagic | grep Location | cut -d" " -f2)/sparkmagic/kernels/sparkkernel
RUN jupyter-kernelspec install --user $(pip show sparkmagic | grep Location | cut -d" " -f2)/sparkmagic/kernels/pysparkkernel
RUN jupyter-kernelspec install --user $(pip show sparkmagic | grep Location | cut -d" " -f2)/sparkmagic/kernels/sparkrkernel
RUN jupyter serverextension enable --py sparkmagic

RUN sed -i 's/from pandas.core.groupby import DataError/from pandas.errors import DataError/g' /opt/conda/lib/python3.10/site-packages/autovizwidget/plotlygraphs/graphbase.py
RUN sed -i 's/from pandas.core.groupby import DataError/from pandas.errors import DataError/g' /opt/conda/lib/python3.10/site-packages/autovizwidget/plotlygraphs/piegraph.py


#########################################################################################
USER root
RUN chown -R $NB_USER:users /home/$NB_USER

USER $NB_USER
CMD ["jupyter", "notebook", "--ip", "0.0.0.0", "--port", "80", "--allow-root"]
