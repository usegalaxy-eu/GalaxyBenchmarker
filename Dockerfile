FROM python:3.10

RUN cd / && git clone https://github.com/usegalaxy-eu/workflow-testing.git

WORKDIR /src

COPY pyproject.toml ./
COPY galaxy_benchmarker ./galaxy_benchmarker

RUN python3 -m pip install --upgrade pip \
  && python3 -m pip install --no-cache-dir .

COPY scripts/entrypoint.sh /entrypoint.sh

CMD ["/entrypoint.sh"]
