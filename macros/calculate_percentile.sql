{% macro calculate_percentile(column_name, percentile) %}
    percentile_cont(fare_amount, {{ percentile }}) OVER (PARTITION BY service_type, year, month)
{% endmacro %}