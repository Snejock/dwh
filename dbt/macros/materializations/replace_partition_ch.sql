{% materialization replace_partition_ch, adapter='trino' %}

    {% set identifier = config.require('identifier') %}
    {{ log('[DEBUG] Variable identifier: ' ~ identifier, info=false) }}

    {% set src_database = model.config.get('source_relation')[0] %}
    {% set src_schema = model.config.get('source_relation')[1] %}
    {% set src_table = model.config.get('source_relation')[2] %}
    {% set source_relation = src_database ~ '.' ~ src_schema ~ '.' ~ src_table %}
    {{ log('[DEBUG] Variable source_relation: ' ~ source_relation, info=false) }}

    {% set trg_database = model.config.get('target_relation')[0] %}
    {% set trg_schema = model.config.get('target_relation')[1] %}
    {% set trg_table = model.config.get('target_relation')[2] %}
    {% set target_relation = api.Relation.create(database=trg_database, schema=trg_schema, identifier=trg_table, type='table') %}
    {{ log('[DEBUG] Variable target_relation: ' ~ target_relation, info=false) }}

    -- log materialization type
    {{ log('[DEBUG] Starting REPLACE_PARTITION_CH materialization for model: ' ~ model.name, info=false) }}

    -- running pre-hooks
    {{ run_hooks(pre_hooks, inside_transaction=false) }}

    -- check existing target relation
    {{ log('[DEBUG] Checking for the target table exists: ' ~ target_relation, info=false) }}
    {% set existing_relation = adapter.get_relation(database=trg_database, schema=trg_schema, identifier=trg_table) %}
    {{ log('[DEBUG] Existing relation: ' ~ existing_relation, info=true) }}

    -- get partition list
    {%- set query -%}
        SELECT array_agg(DISTINCT partition ORDER BY partition)
        FROM {{ trg_database }}.system.parts
        WHERE "database" = '{{ src_schema }}'
          AND "table" = '{{ src_table }}'
    {%- endset -%}
    {%- set response = run_query(query)[0]|replace('<agate.Row: (', '')|replace(')>', '') -%}
    {%- set response = response|replace('"', '')|replace("'", "")|replace('[', '')|replace(']', '') -%}
    {%- set partition_list = response.split(', ') -%}
    {{ log('[DEBUG] Partition_list: ' ~ partition_list, info=true) }}

    -- executing main statement
    {% if existing_relation and check_full_refresh() is false and partition_list|length > 0%}
        {% call statement('main') %}
            {% for partition_expr in partition_list %}
                USE {{ trg_database }}.{{ trg_schema }};
                CALL system.execute(query => 'ALTER TABLE {{ trg_schema }}.{{ trg_table }} REPLACE PARTITION tuple({{ partition_expr|replace("(", "")|replace(")", "") }}) FROM {{ src_schema }}.{{ src_table }}');
            {% endfor %}
        {% endcall %}
    {% endif %}

    -- running post_hooks inside a transaction
    {{ run_hooks(post_hooks, inside_transaction=true) }}

    -- commit transaction
    {{ adapter.commit() }}

    -- running post_hooks outside a transaction
    {{ run_hooks(post_hooks, inside_transaction=false) }}

    -- refresh relations
    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
