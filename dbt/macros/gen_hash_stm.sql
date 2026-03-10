{% macro gen_hash_stm(column, source_system) %}
    {% if column is string %}
        xxhash64(cast(concat_ws('#', coalesce(cast("{{ column }}" as varchar), ''), coalesce(cast('{{ source_system }}' as varchar), '')) as varbinary))
    {% else %}
        xxhash64(cast(concat_ws('#', {% for c in column %} coalesce(cast("{{ c }}" as varchar), '') {{ ',' if not loop.last }}{% endfor %}, coalesce(cast('{{ source_system }}' as varchar), '')) as varbinary))
    {% endif %}
{% endmacro %}
