{% test freshness_custom(model, column_name, max_hours) %}

select max({{ column_name }}) as max_date
from {{ model }}
having max({{ column_name }}) < {{ dbt.current_timestamp() }} - interval '{{ max_hours }} hours'

{% endtest %}