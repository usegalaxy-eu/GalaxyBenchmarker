FROM ubuntu:20.04

RUN \
  apt-get update \
  && apt-get install -y \
    davfs2 \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /tmp/*

COPY scripts/ /scripts

RUN echo "/mnt/volume_under_test rods adminpass" >> /etc/davfs2/secrets

ARG host_owner_uid
RUN adduser --disabled-password --gecos "" --uid "${host_owner_uid}" irods

ENTRYPOINT ["/scripts/entrypoint.sh"]
