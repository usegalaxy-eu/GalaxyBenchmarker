FROM python:3.10

ENV PIP_NO_CACHE_DIR=1 \
  PIP_DISABLE_PIP_VERSION_CHECK=1

RUN \
  apt-get update \
  && apt-get install -y \
    rsync \
    tini \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /tmp/*

RUN pip install poetry

WORKDIR /src
COPY poetry.lock pyproject.toml /src/

RUN poetry config virtualenvs.create false \
  && poetry install --no-dev --no-interaction --no-ansi

RUN git clone https://github.com/usegalaxy-eu/workflow-testing.git /workflow-testing

COPY . /src/

COPY scripts/entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/usr/bin/tini", "-v", "--", "/entrypoint.sh"]
