FROM ubuntu:20.04

ENV MDTEST_VERSION_TAG=3.3.0

RUN \
  apt-get update \
  && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    git automake autoconf make gcc mpich \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /tmp/* \
  && git clone https://github.com/hpc/ior /mdtest \
  && cd /mdtest \
  && git checkout ${MDTEST_VERSION_TAG} \
  && ./bootstrap \
  && mkdir build \
  && cd build \
  && ../configure \
  && make \
  && make install \
  && mkdir /mnt/volume_under_test

WORKDIR /mnt/volume_under_test

ENTRYPOINT [ "mdtest" ]