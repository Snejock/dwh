{%- macro check_full_refresh() -%}
    {%- set config_full_refresh = config.get('full_refresh') -%}
    {%- if config_full_refresh is none -%}
        {%- set config_full_refresh = flags.FULL_REFRESH -%}
    {%- endif -%}
    {%- do return(config_full_refresh) -%}
{%- endmacro -%}
