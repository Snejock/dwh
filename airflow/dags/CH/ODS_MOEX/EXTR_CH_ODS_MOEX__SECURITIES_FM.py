import os

import pendulum

from airflow.sdk import dag
from operators.MOEXToClickhouseOperator import MOEXToClickhouseOperator


dag_id = str(os.path.basename(__file__).replace(".py", ""))

@dag(
    dag_id=dag_id,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags={"moex"}
)
def extract_data():
    MOEXToClickhouseOperator(
        task_id="extract_data",
        url="https://iss.moex.com/iss/securities.json",
        block_json="securities",
        iss_params={"is_trading": 1},
        connection_id="clickhouse_connection",
        trg_schema="ods_moex",
        trg_table="securities_fm",
        order_by_field="loaded_dttm"
    )

extract_data()
