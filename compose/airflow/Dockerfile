# Указываем, базовый образ Airflow
ARG AIRFLOW_IMAGE_NAME=apache/airflow:3.1.2
FROM ${AIRFLOW_IMAGE_NAME}

# Переключение на root для установки системных пакетов
USER root

# Установка Docker CLI, который нужен для DockerOperator
RUN apt-get update && \
    apt-get install -y --no-install-recommends docker-ce-cli && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Создание группы docker и добавляем в неё airflow
ARG DOCKER_GID=986
RUN groupadd -g ${DOCKER_GID} docker || true && usermod -aG docker airflow

# Переключение на пользователя airflow и установка Python-пакетов
USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow-providers-docker==4.4.3" \
    "apache-airflow-providers-trino==6.3.3" \
    "clickhouse-driver==0.2.9" \
    "apache-airflow-providers-fab" \
    "telethon==1.42.0"

USER root