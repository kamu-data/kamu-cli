FROM python:3.10.4-slim

# Tini
ADD https://github.com/krallin/tini/releases/download/v0.19.0/tini /usr/bin/tini
RUN chmod +x /usr/bin/tini

# Script
RUN mkdir /opt/odf &&\
    pip install requests

COPY ingest.py /opt/odf/ingest.py

WORKDIR /opt/odf/
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-m", "ingest"]
