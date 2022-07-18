FROM ubuntu:20.04

## Install official release version

# ENV IRODSFS_VERSION=v0.7.3
# ENV IRODSFS_TAR=irodsfs_amd64_linux_${IRODSFS_VERSION}.tar

# RUN \
#   apt-get update \
#   && apt-get install -y \
#     fuse \
#     wget \
#   && wget https://github.com/cyverse/irodsfs/releases/download/${IRODSFS_VERSION}/${IRODSFS_TAR} \
#   && mkdir /irodsfs \
#   && tar -xf ${IRODSFS_TAR} -C /irodsfs \
#   && rm ${IRODSFS_TAR} \
#   && apt-get clean \
#   && rm -rf /var/lib/apt/lists/* /tmp/*

## Install from specific commit
ENV IRODSFS_COMMIT_SHA=f60d7e16a1ca4f1dcdddc725847ad3095bee92c2

RUN \
  apt-get update \
  && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    fuse \
    wget \
    make \
    git \
    golang-go \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /tmp/* \
  && mkdir /irodsfs.git \
  && git clone https://github.com/cyverse/irodsfs /irodsfs.git \
  && cd /irodsfs.git \
  && git checkout ${IRODSFS_COMMIT_SHA} \
  && make build \
  && mkdir /irodsfs \
  && mv /irodsfs.git/bin/* /irodsfs

COPY scripts/ /scripts

ARG host_owner_uid
RUN adduser --disabled-password --gecos "" --uid "${host_owner_uid}" irods

ENTRYPOINT ["/scripts/entrypoint.sh"]
