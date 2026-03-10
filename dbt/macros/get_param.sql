{#
    /**
     * @macro get_param
     * @description Retrieves a parameter value from the pg.meta.params table.
     *              First attempts to find a parameter specific to the given model name.
     *              If not found, falls back to a global parameter with the same parameter name.
     *              Returns None if no parameter is found in either case.
     *
     * @param {string} model_nm - The name of the model to look up parameters for
     * @param {string} param_nm - The name of the parameter to retrieve
     *
     * @returns {any} The parameter value if found, otherwise None
     *
     * @example
     *   {% set my_param = get_param('my_model', 'my_parameter') %}
     *   {% if my_param is not none %}
     *     -- Use the parameter value
     *   {% endif %}
     */
#}

{% macro get_param(model_nm, param_nm) %}
    {%- set value = None -%}

    {%- if execute -%}
        {%- set model_query -%}
            SELECT value
            FROM pg.meta.params
            WHERE model_nm = '{{ model_nm }}'
              AND param_nm = '{{ param_nm }}'
              AND value IS NOT NULL
        {%- endset -%}

        {%- set results = run_query(model_query) -%}
        {%- if results|length > 0 -%}
            {%- set value = results[0]['value'] -%}
        {%- else -%}
            {%- set global_query -%}
                SELECT value
                FROM pg.meta.params
                WHERE model_nm = 'GLOBAL'
                  AND param_nm = '{{ param_nm }}'
                  AND value IS NOT NULL
            {%- endset -%}

            {%- set results = run_query(global_query) -%}
            {%- if results|length > 0 -%}
                {%- set value = results[0]['value'] -%}
            {%- endif -%}
        {%- endif -%}
    {%- endif -%}

    {{ return(value) }}
{% endmacro %}