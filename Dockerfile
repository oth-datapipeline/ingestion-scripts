# (1) enable venv and install dependencies
FROM python:3.9-slim-bullseye AS base

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

COPY requirements.txt ./requirements.txt

RUN pip install -r requirements.txt

# (2) setup runner image
FROM python:3.9-slim-bullseye AS runner

ENV VIRTUAL_ENV=/opt/venv
ARG scraper_type
ARG base_url

WORKDIR /ingestion_scripts/

COPY --from=base $VIRTUAL_ENV $VIRTUAL_ENV
ENV PATH="/opt/venv/bin:$PATH"

COPY src/ ./src/
COPY test/ ./test/

# (3) start the Faust workers
CMD ./start_faust_instances
