FROM ubuntu:22.04

ENV FIO_SRC_REF=fio-3.30

RUN \
  apt-get update \
  && apt-get install -y \
    build-essential \
    libaio1 \
    libaio-dev \
    git \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /tmp/* \
  && mkdir /fio.git \
  && git clone https://github.com/axboe/fio /fio.git \
  && cd /fio.git \
  && git checkout ${FIO_SRC_REF} \
  && ./configure \
  && make \
  && make install

ENTRYPOINT [ "fio" ]