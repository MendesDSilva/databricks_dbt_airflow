{% macro generate_surrogate_key(columns) %}

    MD5(
        CONCAT_WS(
            '|',
            {% for column in columns %}
                COALESCE(
                    CAST({{ column }} AS STRING),
                    '_dbt_null_'
                )
                {% if not loop.last %},{% endif %}
            {% endfor %}
        )
    )

{% endmacro %}