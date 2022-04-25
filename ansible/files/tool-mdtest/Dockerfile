FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

RUN \
  apt-get update \
  && apt-get install -y \
    git automake autoconf make gcc mpich \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /tmp/* \
  && git clone https://github.com/hpc/ior /mdtest \
  && cd /mdtest \
  && ./bootstrap \
  && mkdir build \
  && cd build \
  && ../configure \
  && make \
  && make install \
  && mkdir /mnt/volume_under_test

WORKDIR /mnt/volume_under_test

ENTRYPOINT [ "mdtest" ]