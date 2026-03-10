{% materialization insert_pg, adapter='trino' %}

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

    {% set primary_key = model.config.get('primary_key') or none %}
    {% set columns = model.config.get('columns') or none %}
    {% set hash_columns = model.config.get('hash_columns') or none %}
    {% set source_system = model.config.get('source_system') or none %}

    -- Verify this is a PostgreSQL target
    {% if not trg_database.endswith('pg') %}
        {{ exceptions.raise_compiler_error('Target database ' ~ trg_database ~ ' is not a PostgreSQL database. Use insert_dlh materialization instead.') }}
    {% endif %}
    {{ log('[DEBUG] Confirmed PostgreSQL target: ' ~ trg_database, info=true) }}

    -- field business and system columns
    {% set sys_columns_list = var('sys_columns_list') %}
    {% set sys_columns = [] %}
    {% set bus_columns = [] %}
    {% for c in columns %}
        {% if c in sys_columns_list %}
            {% set _ = sys_columns.append(c) %}
        {% else %}
            {% set _ = bus_columns.append(c) %}
        {% endif %}
    {% endfor %}
    {{ log('[DEBUG] Variable sys_columns: ' ~ sys_columns, info=false) }}
    {{ log('[DEBUG] Variable bus_columns: ' ~ bus_columns, info=false) }}

    -- log materialization type
    {{ log('[DEBUG] Starting INSERT_PG materialization for model: ' ~ model.name, info=false) }}

    -- running pre-hooks
    {{ run_hooks(pre_hooks, inside_transaction=false) }}

    prepare block
    -- check flag full-refresh
    {% if check_full_refresh() is true %}
        {% do run_query('drop table if exists ' ~ target_relation) %}
        {{ log('[DEBUG] Flag full-refresh dropped table: ' ~ target_relation, info=true) }}
    {% endif %}

    -- check existing target relation
    {{ log('[DEBUG] Checking if the target table exists: ' ~ target_relation, info=false) }}
    {% set existing_relation = adapter.get_relation(database=trg_database, schema=trg_schema, identifier=trg_table) %}
    {{ log('[DEBUG] Existing relation: ' ~ existing_relation, info=true) }}

    -- executing main statement
    {% if existing_relation and check_full_refresh() is false %}
        {% call statement('main') %}
            insert into {{ target_relation }}
                select
                    {% if 'loaded_dttm' in sys_columns %}       cast(now() as timestamp(0))   as loaded_dttm      {% endif %}
                    {% if 'valid_from_dttm' in sys_columns %}   , cast(now() as timestamp(0)) as valid_from_dttm  {% endif %}
                    {% if 'valid_to_dttm' in sys_columns %}     , cast('5999-12-31' as timestamp(0)) as valid_to_dttm {% endif %}
                    {% if 'is_active_flg' in sys_columns %}     , cast(1 as integer)          as is_active_flg    {% endif %}
                    {% if 'is_deleted_flg' in sys_columns %}    , cast(0 as integer)          as is_deleted_flg   {% endif %}
                    {% if 'source_system' in sys_columns %}     , cast('{{ source_system }}' as varchar) as source_system {% endif %}
                    {% if 'row_hash' in sys_columns %}
                        , to_hex(xxhash64(
                            cast(
                                concat_ws('#',
                                    {% for c in bus_columns %}
                                        {% if c[:4] == 'arr_' %}
                                            coalesce(array_join("{{ c }}", ','), '')
                                        {% else %}
                                            coalesce(cast("{{ c }}" as varchar), '')
                                        {% endif %}
                                        {{ ',' if not loop.last }}
                                    {% endfor %}
                                ) as varbinary)
                        )) as row_hash
                    {% endif %}

                    {% if hash_columns is not none %}
                        {% for c in hash_columns %}
                            , {{ gen_hash_stm(c, source_system) }} as {{ gen_hash_alias(c) }}
                        {% endfor %}
                    {% endif %}

                    {% if bus_columns is not none %}
                        {% for c in bus_columns %}
                            , "{{ c }}"
                        {% endfor %}
                    {% endif %}
                from {{ source_relation }};
        {% endcall %}
        {{ log('[DEBUG] Insert completed successfully: ' ~ target_relation, info=false) }}
    {% else %}
        {% call statement('main') %}
            create table {{ target_relation }} (
                {% if 'loaded_dttm' in sys_columns %}       loaded_dttm         timestamp    {% endif %}
                {% if 'valid_from_dttm' in sys_columns %}   , valid_from_dttm   timestamp    {% endif %}
                {% if 'valid_to_dttm' in sys_columns %}     , valid_to_dttm     timestamp    {% endif %}
                {% if 'is_active_flg' in sys_columns %}     , is_active_flg     integer      {% endif %}
                {% if 'is_deleted_flg' in sys_columns %}    , is_deleted_flg    integer      {% endif %}
                {% if 'source_system' in sys_columns %}     , source_system     varchar      {% endif %}
                {% if 'row_hash' in sys_columns %}          , row_hash          varchar      {% endif %}

                {% if hash_columns is not none %}
                    {% for c in hash_columns %}
                        , {{ gen_hash_alias(c) }} varchar
                    {% endfor %}
                {% endif %}

                {% if bus_columns is not none %}
                    {% for c in bus_columns %}
                        , "{{ c }}" {{ get_column_type(src_database, src_schema, src_table, c) }}
                    {% endfor %}
                {% endif %}
            );

            insert into {{ target_relation }}
                select
                    {% if 'loaded_dttm' in sys_columns %}       cast(now() as timestamp(0))     as loaded_dttm      {% endif %}
                    {% if 'valid_from_dttm' in sys_columns %}   , cast(now() as timestamp(0))   as valid_from_dttm  {% endif %}
                    {% if 'valid_to_dttm' in sys_columns %}     , cast('5999-12-31' as timestamp(0)) as valid_to_dttm {% endif %}
                    {% if 'is_active_flg' in sys_columns %}     , cast(1 as integer)            as is_active_flg    {% endif %}
                    {% if 'is_deleted_flg' in sys_columns %}    , cast(0 as integer)            as is_deleted_flg   {% endif %}
                    {% if 'source_system' in sys_columns %}     , cast('{{ source_system }}' as varchar) as source_system {% endif %}
                    {% if 'row_hash' in sys_columns %}
                        , to_hex(xxhash64(
                            cast(
                                concat_ws('#',
                                    {% for c in bus_columns %}
                                        {% if c[:4] == 'arr_' %}
                                            coalesce(array_join("{{ c }}", ','), '')
                                        {% else %}
                                            coalesce(cast("{{ c }}" as varchar), '')
                                        {% endif %}
                                        {{ ',' if not loop.last }}
                                    {% endfor %}
                                ) as varbinary)
                        )) as row_hash
                    {% endif %}

                    {% if hash_columns is not none %}
                        {% for c in hash_columns %}
                            , {{ gen_hash_stm(c, source_system) }} as {{ gen_hash_alias(c) }}
                        {% endfor %}
                    {% endif %}

                    {% if bus_columns is not none %}
                        {% for c in bus_columns %}
                            , "{{ c }}"
                        {% endfor %}
                    {% endif %}
                from {{ source_relation }};
        {% endcall %}
        {{ log('[DEBUG] Table has been created, insert completed successfully: ' ~ target_relation, info=false) }}
        
        {# Add primary key constraint for PostgreSQL #}
        {% if primary_key %}
            {% call statement('add_pk') %}
                CALL {{ trg_database }}.system.execute(
                    query => 'ALTER TABLE {{ trg_schema }}.{{ trg_table }} ADD CONSTRAINT {{ trg_table }}_pk PRIMARY KEY ({{ primary_key | join(", ") }})'
                );
            {% endcall %}
            {{ log('[DEBUG] Added primary key constraint to PostgreSQL table using system.execute', info=true) }}
        {% endif %}
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