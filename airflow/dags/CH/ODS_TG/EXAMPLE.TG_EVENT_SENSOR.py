import os
import pendulum

from airflow.sdk import Variable, dag, task
from sensors.TelegramEventSensor import TelegramEventSensor


dag_id = str(os.path.basename(__file__).replace(".py", ""))

@dag(
    dag_id=dag_id,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags={"telegram"}
)
def wait_for_message():
    sensor_task = TelegramEventSensor(
        task_id="telegram_event_sensor",
        api_id=int(Variable.get("TELEGRAM_API_ID")),
        api_hash=Variable.get("TELEGRAM_API_HASH"),
        channel_username="SNEJ0CK",
    )

    @task(task_id="load_message")
    def load_message(msg: str | None) -> None:
        print(f"Сохраняем в базу: {msg}")

    load_message(sensor_task.output)

wait_for_message()
