FROM ubuntu:20.04

RUN \
  apt-get update \
  && apt-get install -y \
    fuse \
    wget \
  && wget https://github.com/cyverse/irodsfs/releases/download/v0.4.4/irodsfs_amd64_linux_v0.4.4.tar \
  && mkdir /irodsfs \
  && tar -xf irodsfs_amd64_linux_v0.4.4.tar -C /irodsfs \
  && rm irodsfs_amd64_linux_v0.4.4.tar \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /tmp/*

COPY scripts/ /scripts

ARG host_owner_uid
RUN adduser --disabled-password --gecos "" --uid "${host_owner_uid}" irods

ENTRYPOINT ["/scripts/entrypoint.sh"]
