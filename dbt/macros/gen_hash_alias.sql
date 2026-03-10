{% macro gen_hash_alias(column) %}
    {% if column is string %}
        "{{ column }}_hk"
    {% else %}
        "{{ column|join('_') }}_hk"
    {% endif %}
{% endmacro %}
