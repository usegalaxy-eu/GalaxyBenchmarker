FROM python:3.10

ENV PIP_NO_CACHE_DIR=1 \
  PIP_DISABLE_PIP_VERSION_CHECK=1

RUN \
  apt-get update \
  && apt-get install -y \
    fio \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /tmp/*

COPY scripts/ /scripts

ENTRYPOINT [ "fio" ]