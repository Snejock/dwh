import requests
from datetime import datetime
from typing import Any, Optional

from airflow.sdk import BaseOperator
from airflow.sdk import BaseHook
from clickhouse_driver import Client


class MOEXToClickhouseOperator(BaseOperator):
    """Загружает данные из ISS MOEX (JSON) и записывает их в ClickHouse, создавая таблицу
    по схемe, определённой из метаданных ISS.

    Поведение и особенности:
    - Ожидается блок ответа ISS с ключом `block_json`, представляющий таблицу формата ISS:
      объект с полями `columns` (список имён), `metadata` (типы по колонкам) и `data` (массив строк).
    - Типы колонок берутся из `metadata` и маппятся в базовые типы ClickHouse (без Nullable) с помощью
      внутреннего адаптера: string/uuid → String; int8/16/32/64 → Int8/16/32/64; double → Float64;
      datetime → DateTime; bool/boolean → Boolean; прочее → String.
    - Таблица в ClickHouse пересоздаётся: все колонки, кроме `loaded_dttm` и ключа сортировки,
      объявляются как Nullable(<Тип>). `loaded_dttm` всегда имеет тип DateTime и добавляется,
      если отсутствует в исходных данных. Ключ сортировки MergeTree задаётся через `order_by_field`
      (по умолчанию `loaded_dttm`). Если `order_by_field` не указан (None/""), используется `ORDER BY tuple()`.
    - Вставка выполняется батчами (`batch_size`). Для каждой вставляемой строки оператор
      проставляет актуальное значение `loaded_dttm` (Python datetime) в одноимённой колонке.
    - Поддерживается пагинация по параметру `start`: постраничные запросы продолжаются, пока приходят данные
      и если `is_pagination=True`.
    - Подключение к ClickHouse берётся из Airflow Connection (`connection_id`). Если коннект недоступен,
      используется локальный фоллбэк: значения читаются из .env (CH_HOST, CH_PORT, CH_USER, CH_PASSWORD),
      расположенного в корне проекта, вычисленном относительно текущего файла.

    Параметры конструктора:
    :param url: Полный URL ISS (обязателен).
    :param block_json: Имя блока из ответа ISS (обязателен для эндпоинтов с несколькими блоками).
    :param trg_schema: Имя целевой схемы ClickHouse (обязателен).
    :param trg_table: Имя целевой таблицы ClickHouse (обязателен).
    :param iss_params: Параметры запроса ISS (например: iss.only, columns, date, from, till); можно None.
    :param connection_id: Airflow Connection ID для ClickHouse; по умолчанию "clickhouse_connection".
    :param order_by_field: Имя колонки (или None) для ORDER BY в MergeTree.
    :param batch_size: Размер батча вставки; по умолчанию 1_000_000.
    :param timeout: Таймаут HTTP-запроса к ISS в секундах; по умолчанию 30.
    :param is_pagination: Включить постраничную загрузку по `start`; по умолчанию True.
    """

    template_fields = ("iss_params", "url", "block_json", "trg_schema", "trg_table")

    def __init__(
        self,
        url: str,
        block_json: str,
        trg_schema: str,
        trg_table: str,
        iss_params: Optional[dict[str, Any]] | None,
        connection_id: str = "clickhouse_connection",
        order_by_field: str = None,
        batch_size: int = 1_000_000,
        timeout: int = 30,
        is_pagination: bool = True,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        if not url:
            raise ValueError("Field 'url' is required")
        if not block_json:
            raise ValueError("Field 'block' is required for endpoints that can return multiple entities")
        self.url = url
        self.block_json = block_json
        self.iss_params = iss_params or {}
        self.trg_schema = trg_schema
        self.trg_table = trg_table
        self.order_by_field = order_by_field
        self.batch_size = batch_size
        self.timeout = timeout
        self.is_pagination = is_pagination
        if not self.trg_schema:
            raise ValueError("trg_schema is required")
        if not self.trg_table:
            raise ValueError("trg_table is required")

        self.columns: dict[str, str] = {}
        self.data: list[list[Any]] = []
        self.loaded_dttm: datetime | None = None
        self.client = self._get_client(connection_id)

    def execute(self, context: dict[str, Any]) -> None:
        # Получение отметки даты и времени начала процесса
        self.loaded_dttm = datetime.now()

        self.log.info(f"MOEX ISS URL: {self.url}")
        self.log.info(f"ISS params: {self.iss_params}")

        # Получение данных и список колонок
        self._fetch_data(self.url, self.block_json, self.iss_params)
        self.log.info(f"Fetched {len(self.data)} rows from ISS table '{self.block_json}' with {len(self.columns)} columns")
        if not self.data or not self.columns:
            self.log.warning("No data or columns fetched from ISS")
            return

        # Пересоздание таблицы в ClickHouse
        self._recreate_table()

        # Вставка данных в таблицу
        self._insert_batches()
        #
        # self.log.info(f"Successfully inserted {len(data)} records into {self.trg_schema}.{self.trg_table}")

    def _get_client(self, connection_id: str) -> Client:
        try:
            connect = BaseHook.get_connection(connection_id)
        except Exception as e:
            self.log.warning(f"Connection '{connection_id}' not found, using local fallback: {e}")

            from airflow.models.connection import Connection
            from dotenv import dotenv_values
            from pathlib import Path

            project_root = Path(__file__).resolve().parents[3]  # dwh
            env_path = project_root / '.env'

            dot_env = dotenv_values(env_path)
            connect = Connection(
                conn_id='clickhouse_connection',
                conn_type='clickhouse',
                host=dot_env['CH_HOST'],
                schema=self.trg_schema,
                login=dot_env['CH_USER'],
                password=dot_env['CH_PASSWORD'],
                port=int(dot_env['CH_PORT']),
                extra=None,
            )

        return Client(
            host=connect.host,
            port=int(connect.port),
            user=connect.login,
            password=connect.password,
            database=self.trg_schema,
        )

    def _fetch_data(self, url: str, block_json: str, iss_params: dict[str, Any]) -> None:
        start = int(iss_params.get("start", 0))

        while True:
            iss_params["start"] = start

            response = self._http_request(url, params=iss_params)
            block_obj = response.get(block_json)

            if not isinstance(block_obj, dict):
                self.log.error(f"ISS block '{block_json}' is not a dict. Got: {type(block_obj)}. Stopping.")
                break

            columns = block_obj.get("columns", {})
            for c in columns:
                self.columns[c] = self._type_adapter(block_obj["metadata"][c]["type"])

            page_data = block_obj.get("data", [])
            if not isinstance(page_data, list):
                self.log.warning("ISS 'data' is not a list; stopping pagination")
                break

            self.data.extend(page_data)
            if not self.is_pagination or not page_data:
                break
            start += len(page_data)

    def _http_request(self, url: str, params: dict[str, Any]) -> dict[str, Any]:
        response = requests.get(url, params=params, timeout=self.timeout)
        self.log.info(
            f"GET {response.url} -> {response.status_code}; "
            f"payload preview: {response.text[:400].replace('\n', ' ')}... "
        )
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _type_adapter(source_type: str | None) -> str:
        """Адаптер выполняет маппинг типов API ISS -> ClickHouse базовый тип (без Nullable())"""
        if not source_type:
            return "String"

        match source_type.lower():
            # strings
            case "string" | "uuid":
                return "String"

            # integers
            case "int8":
                return "Int8"
            case "int16":
                return "Int16"
            case "int32":
                return "Int32"
            case "int64":
                return "Int64"

            # floats
            case "double":
                return "Float64"

            # dates/time
            case "datetime":
                return "DateTime"
            case "date":
                return "String"

            # boolean
            case "bool" | "boolean":
                return "Bool"

            # fallback
            case _:
                return "String"

    def _recreate_table(self) -> None:
        columns = list(self.columns.keys())
        if "loaded_dttm" not in columns:
            columns = ["loaded_dttm"] + columns

        query = f"""
            DROP TABLE IF EXISTS `{self.trg_schema}`.`{self.trg_table}`
        """
        self.log.info(f"Drop table query: {query}")
        self.client.execute(query)

        column_stm = []
        for col in columns:
            if col == "loaded_dttm":
                column_stm.append(f"`{col}` DateTime")
            elif col == self.order_by_field:
                column_stm.append(f"`{col}` {self.columns[col]}")
            else:
                column_stm.append(f"`{col}` Nullable({self.columns[col]})")

        query = (
            f"CREATE TABLE `{self.trg_schema}`.`{self.trg_table}` ("
            f"{', '.join(column_stm)}) "
            f"ENGINE = MergeTree() "
            f"{f'ORDER BY (`{self.order_by_field}`)' if self.order_by_field else 'ORDER BY tuple()'}"
        )
        self.log.info(f"Create table query: {query}")
        self.client.execute(query)

    def _insert_batches(self) -> None:
        columns = list(self.columns.keys())
        if "loaded_dttm" not in columns:
            columns = ["loaded_dttm"] + columns
            self.data = [[self.loaded_dttm] + row for row in self.data]

        query = (
            f"INSERT INTO `{self.trg_schema}`.`{self.trg_table}` "
            f"({', '.join([f'`{c}`' for c in columns])}) VALUES"
        )

        total_inserted_rows = 0
        for i in range(0, len(self.data), self.batch_size):
            batch = self.data[i:i + self.batch_size]
            self.client.execute(query, batch)
            total_inserted_rows += len(batch)
            self.log.info(f"Inserted batch {i+1}; total inserted: {total_inserted_rows} rows.")
