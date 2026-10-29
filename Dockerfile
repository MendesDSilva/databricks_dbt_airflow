FROM astrocrpublic.azurecr.io/runtime:3.3-11

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /usr/local/airflow/dbt_venv

COPY dbt-requirements.txt /tmp/dbt-requirements.txt

RUN /usr/local/airflow/dbt_venv/bin/pip install \
    --no-cache-dir \
    -r /tmp/dbt-requirements.txt

USER astro