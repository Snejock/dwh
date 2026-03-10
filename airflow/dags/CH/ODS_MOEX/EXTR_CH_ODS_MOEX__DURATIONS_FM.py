import os

from datetime import datetime

from airflow.sdk import dag
from operators.MOEXToClickhouseOperator import MOEXToClickhouseOperator

dag_id = str(os.path.basename(__file__).replace(".py", ""))


@dag(
    dag_id=dag_id,
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags={"moex"}
)
def extract_data():
    MOEXToClickhouseOperator(
        task_id="extract_data",
        url="https://iss.moex.com/iss/index.json",
        block_json="durations",
        iss_params={},
        connection_id="clickhouse_connection",
        trg_schema="ods_moex",
        trg_table="durations_fm",
        order_by_field="loaded_dttm",
        is_pagination=False
    )

extract_data()
