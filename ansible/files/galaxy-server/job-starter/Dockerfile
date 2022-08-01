FROM python:3.10-slim

# Runtime for galaxy scripts

RUN \
  apt-get update \
  && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    git \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /tmp/*

WORKDIR /scripts
COPY scripts/requirements.txt /scripts

RUN python -m pip install -r requirements.txt

COPY scripts/ /scripts/

ENTRYPOINT ["/scripts/entrypoint.sh"]
