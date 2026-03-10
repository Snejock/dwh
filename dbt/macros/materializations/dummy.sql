{% materialization dummy, default %}

{{ run_hooks(pre_hooks, inside_transaction=false) }}
-- begin happens here:
{{ run_hooks(pre_hooks, inside_transaction=true) }}

{%- if pre_hooks| length > 0 -%}
    {%- set pre_comment = '/*\nPRE_HOOK_RUN:\n' -%} 
    {%- if pre_hooks is string -%}
        {%- set pre_comment = pre_comment ~ render(pre_hooks[0]['sql']) ~ '\n*/' -%}
    {%- else -%}
        {%- set pre_comment = pre_comment ~ pre_hooks | join("\n", attribute="sql") -%}
        {%- set pre_comment = render(pre_comment) ~ '\n*/' -%}   {%-endif-%}
    {%- set sql = pre_comment ~ sql -%}
{%-endif-%}

{%- if post_hooks| length > 0 -%}
    {%- set post_comment = '\n/*\nPOST_HOOK_RUN:\n' -%} 
    {%- if post_hooks is string -%}
        {%- set post_comment = post_comment ~ render(post_hooks[0]['sql']) ~ '\n*/' -%}
    {%- else -%}
        {%- set post_comment = post_comment ~ post_hooks | join("\n", attribute="sql") -%}
        {%- set post_comment = render(post_comment) ~ '\n*/' -%}
    {%-endif-%}
    {%- set sql = sql ~ post_comment -%}
{%-endif-%}

-- build model
{% call statement('main') -%}
    {{ sql }}
{%- endcall %}

-- running post_hooks inside a transaction
{{ run_hooks(post_hooks, inside_transaction=true) }}

-- commit happens here
{{ adapter.commit() }}

-- running post_hooks outside a transaction
{{ run_hooks(post_hooks, inside_transaction=false) }}

{{ return({'relations': [this]}) }}

{% endmaterialization %}
