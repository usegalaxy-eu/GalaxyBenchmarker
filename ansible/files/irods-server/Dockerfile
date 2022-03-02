FROM ubuntu:18.04

RUN \
  apt-get update \
  && apt-get install -y \
    expect \
    gnupg \
    lsb-release \
    wget \
  && wget -qO - https://packages.irods.org/irods-signing-key.asc | apt-key add - \
  && echo "deb [arch=amd64] https://packages.irods.org/apt/ $(lsb_release -sc) main" | tee /etc/apt/sources.list.d/renci-irods.list \
  && apt-get update \
  && apt-get install -y \
    irods-server \
    irods-database-plugin-postgres \
  && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/* /tmp/*

COPY scripts/ /scripts

RUN adduser --disabled-password --gecos "" --uid 1000 irods

ENTRYPOINT ["/scripts/entrypoint.sh"]