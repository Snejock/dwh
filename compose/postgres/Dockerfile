FROM postgres:18.6-trixie

# Установка зависимостей для сборки
RUN apt-get update && apt-get install -y \
    git \
    make \
    gcc \
    postgresql-server-dev-18 \
    && rm -rf /var/lib/apt/lists/*

# Установка расширения PG vector
RUN git clone --branch v0.8.6 https://github.com/pgvector/pgvector.git /tmp/pgvector \
    && cd /tmp/pgvector \
    && make \
    && make install \
    && rm -rf /tmp/pgvector

# Установка расширения PG xxhash
RUN git clone --branch master https://github.com/hatarist/pg_xxhash.git /tmp/pg_xxhash \
    && cd /tmp/pg_xxhash \
    && make \
    && make install \
    && rm -rf /tmp/pg_xxhash

RUN apt-get purge -y git make gcc postgresql-server-dev-18 && apt-get autoremove -y
