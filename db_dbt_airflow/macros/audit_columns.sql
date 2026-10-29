{% macro audit_columns()%}

    CURRENT_TIMESTAMP() as dbt_load_at,
    '{{invocation_id}}' as dbt_run_id

{% endmacro %}