{%- macro get_column_type(database, schema, table, column) -%}
    {%- set query -%}
        select data_type
        from {{ database }}.information_schema.columns
        where table_schema = '{{ schema }}'
          and table_name = '{{ table }}'
          and column_name = '{{ column }}'
    {%- endset -%}

    {%- set results = run_query(query) -%}
    {%- if execute -%}
        {%- for row in results -%}
            {{- row.data_type -}}
        {%- endfor -%}
    {%- endif -%}
{%- endmacro -%}
