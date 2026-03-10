from __future__ import annotations

import asyncio
from telethon import TelegramClient, events
from airflow.sdk import BaseSensorOperator
from airflow.triggers.base import TriggerEvent, BaseTrigger


class TelegramMessageTrigger(BaseTrigger):
    def __init__(self, api_id: int, api_hash: str, channel_username: str, session_name: str):
        super().__init__()
        self.api_id = api_id
        self.api_hash = api_hash
        self.channel_username = channel_username
        self.session_name = session_name

    def serialize(self):
        """Airflow сериализует триггеры, чтобы запустить их в отдельном процессе."""
        return (
            "plugins.sensors.TelegramEventSensor.TelegramMessageTrigger",
            {
                "api_id": self.api_id,
                "api_hash": self.api_hash,
                "channel_username": self.channel_username,
                "session_name": self.session_name,
            },
        )

    async def run(self):
        """Асинхронный генератор, который создает событие на каждое новое сообщение."""
        client = TelegramClient(self.session_name, self.api_id, self.api_hash)
        await client.start()

        queue: asyncio.Queue = asyncio.Queue()

        @client.on(events.NewMessage(chats=self.channel_username))
        async def handler(ev):
            await queue.put(ev.message.message or "")

        try:
            while True:
                msg = await queue.get()
                yield TriggerEvent({"message": msg})
        finally:
            await client.disconnect()


class TelegramEventSensor(BaseSensorOperator):
    """
    Deferrable-сенсор, который ждёт событие в Telegram.
    """

    def __init__(self, *, api_id: int, api_hash: str, channel_username: str, session_name: str = "/opt/airflow/config/tg_sensor", **kwargs):
        super().__init__(**kwargs)
        self.api_id = api_id
        self.api_hash = api_hash
        self.channel_username = channel_username
        self.session_name = session_name
        self.deferrable = True

    def execute(self, context):
        self.defer(
            trigger=TelegramMessageTrigger(
                api_id=self.api_id,
                api_hash=self.api_hash,
                channel_username=self.channel_username,
                session_name=self.session_name,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context, event=None):
        """Метод, вызываемый, когда Trigger вернул результат."""
        if event and "message" in event:
            message = event["message"]
            context["ti"].xcom_push(key="telegram_message", value=message)
            self.log.info("Получено сообщение из Telegram: %s", message)
        return True
