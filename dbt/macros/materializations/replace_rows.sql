{% materialization replace_rows, adapter='trino' %}

    {% set identifier = config.require('identifier') %}
    {{ log('[DEBUG] Variable identifier: ' ~ identifier, info=true) }}

    {% set src_database = model.config.get('source_relation')[0] %}
    {% set src_schema = model.config.get('source_relation')[1] %}
    {% set src_table = model.config.get('source_relation')[2] %}
    {% set source_relation = src_database ~ '.' ~ src_schema ~ '.' ~ src_table %}
    {{ log('[DEBUG] Variable source_relation: ' ~ source_relation, info=true) }}

    {% set trg_database = model.config.get('target_relation')[0] %}
    {% set trg_schema = model.config.get('target_relation')[1] %}
    {% set trg_table = model.config.get('target_relation')[2] %}
    {% set target_relation = api.Relation.create(identifier=trg_table, schema=trg_schema, database=trg_database, type='table') %}
    {{ log('[DEBUG] Variable target_relation: ' ~ target_relation, info=true) }}

    {% set primary_key = model.config.get('primary_key') or none %}
    {% set sorted_by = model.config.get('sorted_by') or none %}
    {% set partition_by = model.config.get('partition_by') or none %}
    {% set columns = model.config.get('columns') or none %}
    {% set hash_columns = model.config.get('hash_columns') or none %}
    {% set source_system = model.config.get('source_system') or none %}
    {% set min_replace_dttm = get_min_value(column='edr_dttm', table_nm=source_relation) %}
    {% set max_replace_dttm = get_max_value(column='edr_dttm', table_nm=source_relation) %}

    -- field business and system columns
    {% set sys_columns_list = var('sys_columns_list') %}
    {% set sys_columns = [] %}
    {% set pk_columns = [] %}
    {% set att_columns = [] %}

    {% for c in columns %}
        {% if c in sys_columns_list %}
            {% set _ = sys_columns.append(c) %}
        {% elif c in primary_key %}
            {% set _ = pk_columns.append(c) %}
        {% else %}
            {% set _ = att_columns.append(c) %}
        {% endif %}
    {% endfor %}

    {{ log('[DEBUG] Variable sys_columns: ' ~ sys_columns, info=true) }}
    {{ log('[DEBUG] Variable pk_columns: ' ~ pk_columns, info=true) }}
    {{ log('[DEBUG] Variable att_columns: ' ~ att_columns, info=true) }}

    -- log materialization type
    {{ log('[DEBUG] Starting REPLACE_ROWS materialization for model: ' ~ model.name, info=false) }}

    -- running pre-hooks
    {{ run_hooks(pre_hooks, inside_transaction=false) }}

    -- prepare block
    -- check flag full-refresh
    {% if check_full_refresh() is true %}
        {% do run_query('DROP TABLE IF EXISTS ' ~ target_relation) %}
        {{ log('[DEBUG] Flag full-refresh dropped table: ' ~ target_relation, info=true) }}
    {% endif %}

    -- check existing target relation
    {{ log('[DEBUG] Checking for the target table exists: ' ~ target_relation, info=false) }}
    {% set existing_relation = adapter.get_relation(database=trg_database, schema=trg_schema, identifier=trg_table) %}
    {{ log('[DEBUG] Existing relation: ' ~ existing_relation, info=true) }}

    -- check existing data in source relation
    {{ log('[DEBUG] Checking for exists data in source relation: ' ~ target_relation, info=false) }}
    {% if min_replace_dttm is not none and max_replace_dttm is not none %}
        {% set existing_source_data = true %}
    {% else %}
        {% set existing_source_data = false %}
    {% endif %}
    {{ log('[DEBUG] Existing source relation data: ' ~ existing_source_data, info=true) }}
    {{ log('[DEBUG] Replacing period from: ' ~ min_replace_dttm ~ ', replacing period to: ' ~ max_replace_dttm, info=true) }}

    -- deleting replacing rows by datetime period
    {% if existing_source_data and existing_relation %}
        {% call statement('main') %}
            DELETE FROM {{ target_relation }}
            WHERE timestamp'{{ min_replace_dttm }}' <= edr_dttm AND edr_dttm <= timestamp'{{ max_replace_dttm }}';
        {% endcall %}
    {% endif %}

    -- executing main statement
    {% if existing_relation and check_full_refresh() is false %}
        {% call statement('main') %}
            insert into {{ target_relation }}
                select
                    {% if 'loaded_dttm' in sys_columns %}       cast(current_timestamp AS timestamp(0)),   {% endif %}
                    {% if 'source_system' in sys_columns %}     source_system,         {% endif %}

                    {% if pk_columns is not none %}
                        {% for c in pk_columns %}
                            "{{ c }}",
                        {% endfor %}
                    {% endif %}

                    {% if hash_columns is not none %}
                        {% for c in hash_columns %}
                            {{ gen_hash_alias(c) }},
                        {% endfor %}
                    {% endif %}

                    {% if att_columns is not none %}
                        {% for c in att_columns %}
                            "{{ c }}" {{ ',' if not loop.last }}
                        {% endfor %}
                    {% endif %}
                from {{ source_relation }}
            ;
        {% endcall %}
        {{ log('[DEBUG] Materialize replace_rows completed successfully: ' ~ target_relation, info=false) }}
    {% else %}
        {% call statement('main') %}
            CREATE TABLE {{ target_relation }} (
                {% if 'loaded_dttm'        in sys_columns %}  loaded_dttm         timestamp(0),    {% endif %}
                {% if 'source_system'      in sys_columns %}  source_system       varchar,         {% endif %}

                {% if pk_columns is not none %}
                    {% for c in pk_columns %}
                        "{{ c }}" {{ get_column_type(src_database, src_schema, src_table, c) }},
                    {% endfor %}
                {% endif %}

                {% if hash_columns is not none %}
                    {% for c in hash_columns %}
                        {{ gen_hash_alias(c) }} varbinary,
                    {% endfor %}
                {% endif %}

                {% if att_columns is not none %}
                    {% for c in att_columns %}
                        "{{ c }}" {{ get_column_type(src_database, src_schema, src_table, c) }} {{ ',' if not loop.last }}
                    {% endfor %}
                {% endif %}
            )
            WITH (
                format = 'PARQUET'
                {% if sorted_by is not none %}
                    , sorted_by = ARRAY{{ sorted_by }}
                {% endif %}
                {% if partition_by is not none %}
                    , partitioning = ARRAY{{ partition_by }}
                {% endif %}
            );

            INSERT INTO {{ target_relation }}
                SELECT
                    {% if 'loaded_dttm' in sys_columns %}       cast(now() AS timestamp(0))   AS loaded_dttm,      {% endif %}
                    {% if 'source_system' in sys_columns %}     source_system, {% endif %}

                    {% if pk_columns is not none %}
                        {% for c in pk_columns %}
                            src."{{ c }}",
                        {% endfor %}
                    {% endif %}

                    {% if hash_columns is not none %}
                        {% for c in hash_columns %}
                            {{ gen_hash_stm(c, source_system) }} AS {{ gen_hash_alias(c) }},
                        {% endfor %}
                    {% endif %}

                    {% if att_columns is not none %}
                        {% for c in att_columns %}
                            src."{{ c }}" {{ ',' if not loop.last }}
                        {% endfor %}
                    {% endif %}
                FROM {{ source_relation }} AS src;
        {% endcall %}
        {{ log('[DEBUG] Table has been created, insert completed successfully: ' ~ target_relation, info=false) }}
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
