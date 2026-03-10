{% macro set_param(model_nm, column_nm, param_nm, table_nm) %}
    {% if execute %}
        {% set get_value_query %}
            SELECT cast(max({{ column_nm }}) as varchar)
            FROM {{ table_nm }}
        {% endset %}
        {% set result = run_query(get_value_query) %}

        {% do log('[DEBUG] query getting value for set_param:\n' ~ get_value_query) %}
        {% do log('[DEBUG] value: ' ~ result) %}

        {% if result and result.columns[0].values() %}
            {% set value = result.columns[0].values()[0] %}
        {% else %}
            {% do log('[DEBUG] No result value for set_param returned from the query') %}
            {% set value = '' %}
        {% endif %}

        {% set update_query %}
            UPDATE pg.meta.params
            SET value = '{{ value }}'
            WHERE param_nm = '{{ param_nm }}'
              AND model_nm = '{{ model_nm }}';
        {% endset %}

        {% do log('[DEBUG] Update meta params query: \n' ~ update_query) %}
        {% do run_query(update_query) %}
    {% endif %}
{%- endmacro %}
