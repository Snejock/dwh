FROM ghcr.io/dbt-labs/dbt-core:1.11.11

WORKDIR /usr/app/dbt/dwh
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
