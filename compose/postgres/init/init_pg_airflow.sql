-- База данных для метаданных Airflow. Отдельная роль не заводится —
-- владелец и все права у общего пользователя core (см. POSTGRES__USER/
-- POSTGRES__PASSWORD в .env; та же учётка используется в
-- AIRFLOW__DATABASE__SQL_ALCHEMY_CONN в compose/airflow/docker-compose.yml).
CREATE DATABASE airflow OWNER core;

\c airflow

GRANT ALL PRIVILEGES ON SCHEMA public TO core;