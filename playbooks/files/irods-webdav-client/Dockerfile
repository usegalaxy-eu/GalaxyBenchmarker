FROM ubuntu:20.04

RUN \
  apt-get update \
  && apt-get install -y \
    davfs2 \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /tmp/*

COPY scripts/ /scripts

RUN echo "/mnt/volume_under_test rods adminpass" >> /etc/davfs2/secrets

RUN adduser --disabled-password --gecos "" --uid 1000 irods

ENTRYPOINT ["/scripts/entrypoint.sh"]
