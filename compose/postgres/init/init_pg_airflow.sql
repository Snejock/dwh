-- Создаем пользователя airflow с паролем airflow
CREATE USER airflow WITH PASSWORD 'airflow';

-- Создаем базу данных airflow, владельцем которой будет пользователь airflow
CREATE DATABASE airflow OWNER airflow;

GRANT ALL PRIVILEGES ON DATABASE airflow TO core;


-- Сначала подключаемся к созданной базе данных airflow, чтобы контекст был верным
-- Это гарантирует, что следующие команды GRANT и ALTER DEFAULT PRIVILEGES
-- применяются именно к базе данных 'airflow'
\c airflow

-- Даем пользователю core права на использование схемы public и создание объектов в ней
GRANT USAGE, CREATE ON SCHEMA public TO core;

-- Даем все права на все существующие таблицы в схеме public
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO core;

-- Даем все права на все существующие последовательности в схеме public
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO core;

-- Даем все права на все существующие функции в схеме public
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO core;

-- Важно: Устанавливаем права по умолчанию для будущих объектов,
-- которые могут быть созданы (например, пользователем airflow) в схеме public.
-- Это гарантирует, что 'core' будет иметь к ним доступ.
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO core;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON SEQUENCES TO core;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON FUNCTIONS TO core;