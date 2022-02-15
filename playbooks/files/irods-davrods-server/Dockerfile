FROM ubuntu:18.04
RUN apt-get update \
  && apt-get install -y \
    apache2 \
    apache2-utils \
    gnupg \
    lsb-release \
    wget \
  && wget -qO - https://packages.irods.org/irods-signing-key.asc | apt-key add - \
  && echo "deb [arch=amd64] https://packages.irods.org/apt/ $(lsb_release -sc) main" | tee /etc/apt/sources.list.d/renci-irods.list \
  && apt-get update \
  && apt-get install -y \
    irods-runtime=4.2.10 \
  && wget -qO - https://github.com/UtrechtUniversity/davrods/releases/download/4.2.10_1.5.0/davrods-4.2.10-1.5.0.deb > /tmp/davrods.deb \
  && apt install /tmp/davrods.deb \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /tmp/*

COPY ./volumes/davrods-vhost.conf /etc/apache2/sites-available/davrods-vhost.conf

RUN a2enmod dav \
  && a2enmod davrods \
  && a2ensite davrods-vhost

EXPOSE 80
ENTRYPOINT ["apache2ctl"]
CMD ["-DFOREGROUND"]


# COPY scripts/ /scripts

# RUN adduser --disabled-password --gecos "" --uid 1000 irods

# ENTRYPOINT ["/scripts/entrypoint.sh"]