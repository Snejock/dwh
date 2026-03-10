{% macro get_max_value(column, table_nm) %}
    {%- set value = None -%}

    {%- if execute -%}
        {%- set query -%}
            SELECT max({{ column }}) AS value
            FROM {{ table_nm }}
        {%- endset -%}

        {%- set results = run_query(query) -%}
        {%- if results|length > 0 -%}
            {%- set value = results[0]['value'] -%}
        {%- endif -%}
        {% do log('[DEBUG] get_max_value(' ~ column ~', ' ~ table_nm ~ '): ' ~ value) %}
    {%- endif -%}

    {{ return(value) }}
{% endmacro %}