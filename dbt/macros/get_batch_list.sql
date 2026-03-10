{% macro get_batch_list(query, batch_size=1000, id_column_index=0) %}
    {#-
        Запускает переданный SQL-запрос и делит результат колонки, указанной в id_column_index на батчи.
        Args:
            query (str): SQL-запрос, который возвращает ID для разделения на батчи.
                         Важно, чтобы нужные ID были в *первой* (или указанной `id_column_index`) колонке.
            batch_size (int): Желаемый размер каждого батча.
            id_column_index (int): Индекс (начиная с 0) колонки в результатах `query`, содержащей ID.
        Returns:
            list[list]: Список списков, где каждый вложенный список - это батч ID.
                        Возвращает пустой список, если запрос не вернул результатов или во время парсинга.
    -#}

    {% set batches = [] %}

    {% if execute %}
        {{ log("[DEBUG] Executing query to get iteration items:\n" ~ query, info=True) }}
        {% set results = run_query(query) %}

        {# Проверяем, есть ли результаты и нужная колонка #}
        {% if results and results.columns | length > id_column_index %}
            {# Извлекаем значения ID из указанной колонки #}
            {% set ids = results.columns[id_column_index].values() | list %}
            {% set total_ids = ids | length %}
            {{ log("[DEBUG] Found items for batching: " ~ total_ids, info=True) }}

            {# Делим список ID на батчи #}
            {% for i in range(0, total_ids, batch_size) %}
                {% set current_batch = ids[i:i+batch_size] %}
                {% if current_batch %} {# Убедимся, что батч не пустой #}
                    {# Добавляем текущий батч (список ID) в общий список батчей #}
                    {% do batches.append(current_batch) %}
                {% endif %}
            {% endfor %}
            {{ log("[DEBUG] Divided into " ~ batches | length ~ " batches.", info=True) }}
        {% else %}
            {{ log("[DEBUG] Warning: Query didn't return results or column with index " ~ id_column_index ~ " not found.", info=True) }}
        {% endif %}
    {% else %}
        {{ log("[DEBUG] Parsing mode, skipping query execution.", info=True) }}
    {% endif %}

    {# Возвращаем список батчей #}
    {{ return(batches) }}

{% endmacro %}