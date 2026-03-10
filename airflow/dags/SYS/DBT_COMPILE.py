import logging
import os

from datetime import datetime

from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator


dag_id = str(os.path.basename(__file__).replace(".py", ""))

@dag(
    dag_id=dag_id,
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sys"],
)
def dbt_compile():
    BashOperator(
        task_id="dbt_compile",
        bash_command=(
            f"docker exec dwh_dbt dbt compile "
            f"--target {os.environ.get('DBT_TARGET', 'prod')} "
            f"--profiles-dir {os.environ.get('DBT_PROFILES_DIR')} "
            f"--project-dir {os.environ.get('DBT_PROJECT_DIR')}"
        ),
        # env={
        #     'DBT_USER': '{{ conn.postgres.login }}',
        #     'DBT_ENV_SECRET_PASSWORD': '{{ conn.postgres.password }}',
        #     'DBT_HOST': '{{ conn.postgres.host }}',
        #     'DBT_SCHEMA': '{{ conn.postgres.schema }}',
        #     'DBT_PORT': '{{ conn.postgres.port }}',
        # },
    )

    logging.info('DBT compile was completed successfully')

dbt_compile()
