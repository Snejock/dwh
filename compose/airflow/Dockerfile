# Указываем, базовый образ Airflow
ARG AIRFLOW_IMAGE_NAME=apache/airflow:3.3.1
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

# Заранее создаём стандартные директории Airflow с правильным владельцем.
# Это важно для именованных docker volume: при первом использовании Docker
# наполняет пустой volume содержимым из этого пути в образе, включая права.
RUN mkdir -p /opt/airflow/{dags,logs,plugins,config} && \
    chown -R airflow:root /opt/airflow/{dags,logs,plugins,config}

# Переключение на пользователя airflow и установка Python-пакетов
USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow-providers-docker==4.4.3" \
    "apache-airflow-providers-trino==6.3.3" \
    "clickhouse-driver==0.2.11" \
    "apache-airflow-providers-fab"

# Заранее компилируем весь /home/airflow/.local (весь --user-site: и базовый
# airflow-образ, и пакеты выше) в .pyc прямо в образе. Без этого шага Python
# компилирует байткод заново при каждом холодном старте интерпретатора —
# `import airflow` на этом стеке весит десятки тысяч файлов и без готового
# кеша занимает 30-40+ секунд CPU-времени. Это заметно на любом сервисе,
# который часто поднимает новый процесс (healthcheck каждые 30с, парсинг
# DAG'ов подпроцессами). `-j 0` — компилировать параллельно по числу ядер.
# `|| true` — не валить сборку, если compileall споткнётся о единичный файл.
RUN python -m compileall -q -j 0 /home/airflow/.local || true

USER root