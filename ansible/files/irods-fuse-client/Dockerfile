FROM ubuntu:20.04

ENV IRODSFS_VERSION=v0.6.0
ENV IRODSFS_TAR=irodsfs_amd64_linux_${IRODSFS_VERSION}.tar

RUN \
  apt-get update \
  && apt-get install -y \
    fuse \
    wget \
  && wget https://github.com/cyverse/irodsfs/releases/download/${IRODSFS_VERSION}/${IRODSFS_TAR} \
  && mkdir /irodsfs \
  && tar -xf ${IRODSFS_TAR} -C /irodsfs \
  && rm ${IRODSFS_TAR} \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /tmp/*

COPY scripts/ /scripts

ARG host_owner_uid
RUN adduser --disabled-password --gecos "" --uid "${host_owner_uid}" irods

ENTRYPOINT ["/scripts/entrypoint.sh"]
