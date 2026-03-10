{% materialization append, adapter='trino' %}

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
    {{ log('[DEBUG] Starting APPEND materialization for model: ' ~ model.name, info=true) }}

    -- running pre-hooks
    {{ run_hooks(pre_hooks, inside_transaction=false) }}

    -- prepare block
    -- check flag full-refresh
    {% if check_full_refresh() is true %}
        {% do run_query('DROP TABLE IF EXISTS ' ~ target_relation) %}
        {{ log('[DEBUG] Flag full-refresh dropped table: ' ~ target_relation, info=True) }}
    {% endif %}

    -- check existing target relation
    {{ log('[DEBUG] Checking if the target table exists: ' ~ target_relation, info=true) }}
    {% set existing_relation = adapter.get_relation(database=trg_database, schema=trg_schema, identifier=trg_table) %}
    {{ log('[DEBUG] Existing relation: ' ~ existing_relation, info=True) }}

    -- executing main statement
    {% if existing_relation and check_full_refresh() is false %}
        {% call statement('main') %}

            -- insert new records
            MERGE INTO {{ target_relation }} AS trg
            USING (
                SELECT
                    {% if 'source_system' in sys_columns %}     source_system,        {% endif %}
                    {% for c in pk_columns %}                   "{{ c }}",            {% endfor %}
                    {% if 'row_hash' in sys_columns %}          row_hash,             {% endif %}

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
                FROM {{ source_relation }}
            ) AS src
            ON {% for c in pk_columns %} trg."{{ c }}" = src."{{ c }}" {{ ',' if not loop.last }} {% endfor %}
               {% if 'row_hash' in sys_columns %} AND trg.row_hash = src.row_hash {% endif %}
            WHEN NOT MATCHED
            THEN INSERT (
                    {% if 'loaded_dttm' in sys_columns %}       loaded_dttm,           {% endif %}
                    {% if 'valid_from_dttm' in sys_columns %}   valid_from_dttm,       {% endif %}
                    {% if 'is_deleted_flg' in sys_columns %}    is_deleted_flg,        {% endif %}
                    {% if 'source_system' in sys_columns %}     source_system,         {% endif %}
                    {% if 'row_hash' in sys_columns %}          row_hash,              {% endif %}

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
                )
                VALUES (
                    {% if 'loaded_dttm' in sys_columns %}       cast(current_timestamp AS timestamp(0)),   {% endif %}
                    {% if 'valid_from_dttm' in sys_columns %}   cast(current_timestamp AS timestamp(0)),   {% endif %}
                    {% if 'is_deleted_flg' in sys_columns %}    cast(0 AS integer),                        {% endif %}
                    {% if 'source_system' in sys_columns %}     cast('{{ source_system }}' AS varchar),    {% endif %}
                    {% if 'row_hash' in sys_columns %}          src.row_hash,                              {% endif %}

                    {% if pk_columns is not none %}
                        {% for c in pk_columns %}
                            src."{{ c }}",
                        {% endfor %}
                    {% endif %}

                    {% if hash_columns is not none %}
                        {% for c in hash_columns %}
                            {{ gen_hash_stm(c, source_system) }},
                        {% endfor %}
                    {% endif %}

                    {% if att_columns is not none %}
                        {% for c in att_columns %}
                            src."{{ c }}" {{ ',' if not loop.last }}
                        {% endfor %}
                    {% endif %}
                );

        {% endcall %}
        {{ log('[DEBUG] Append completed successfully: ' ~ target_relation, info=true) }}
    {% else %}
        {% call statement('main') %}
            CREATE TABLE {{ target_relation }} (
                {% if 'loaded_dttm'        in sys_columns %}  loaded_dttm         timestamp(0),    {% endif %}
                {% if 'valid_from_dttm'    in sys_columns %}  valid_from_dttm     timestamp(0),    {% endif %}
                {% if 'is_deleted_flg'     in sys_columns %}  is_deleted_flg      integer,         {% endif %}
                {% if 'source_system'      in sys_columns %}  source_system       varchar,         {% endif %}
                {% if 'row_hash'           in sys_columns %}  row_hash            varbinary,       {% endif %}

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
                    {% if 'valid_from_dttm' in sys_columns %}   cast(now() AS timestamp(0))   AS valid_from_dttm,  {% endif %}
                    {% if 'is_deleted_flg' in sys_columns %}    cast(0 AS integer)            AS is_deleted_flg,   {% endif %}
                    {% if 'source_system' in sys_columns %}     cast('{{ source_system }}' AS varchar) AS source_system, {% endif %}
                    {% if 'row_hash' in sys_columns %}          to_hex(xxhash64(cast(concat_ws('#', {% for c in att_columns %} coalesce(cast(src."{{ c }}" AS varchar), '') {{ ',' if not loop.last }}{% endfor %}) AS varbinary))) AS row_hash, {% endif %}

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
        {{ log('[DEBUG] Table has been created, insert completed successfully: ' ~ target_relation, info=true) }}
    {% endif %}

    -- running post_hooks
    {{ run_hooks(post_hooks, inside_transaction=false) }}

    -- commit transaction
    {{ adapter.commit() }}

    -- refresh relations
    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}