{% macro check_required_columns(sys_columns, required_columns) %}
    {% set missing_columns = [] %}

    {% for column in required_columns %}
        {% if column not in sys_columns %}
            {% do missing_columns.append(column) %}
        {% endif %}
    {% endfor %}

    {% if missing_columns|length > 0 %}
        {% do exceptions.raise_compiler_error('Missing required columns: ' ~ missing_columns|join(', ')) %}
    {% endif %}
{% endmacro %}