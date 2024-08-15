{% test column_completeness_test(model, column_name, origin_model, origin_column, filter=none, origin_filter=none) %}


with validation as (

    select
        o.{{ origin_column }},
        m.{{ column_name }} as match_value 
    from
      {{ ref(origin_model) }} o
        LEFT JOIN
      {{ model }} m
          ON o.{{ origin_column }} = m.{{ column_name }}
    where
      o.{{ origin_column }} is not null
      {% if origin_filter %}
      and {{ origin_filter }}
      {% endif %}
      {% if filter %}
      and {{ filter }}
      {% endif %}

),

validation_errors as (

    select
      {{ origin_column }}
    from validation
    where match_value is null

)

select *
from validation_errors

{% endtest %}


{% test column_completeness_test_source(model, column_name, source_name, source_table, origin_column, filter=none, origin_filter=none) %}


with validation as (

    select
        o.{{ origin_column }},
        m.{{ column_name }} as match_value 
    from
      {{ source(source_name, source_table) }} o
        LEFT JOIN
      {{ model }} m
          ON o.{{ origin_column }} = m.{{ column_name }}
    where
      o.{{ origin_column }} is not null
      {% if origin_filter %}
      and {{ origin_filter }}
      {% endif %}
      {% if filter %}
      and {{ filter }}
      {% endif %}

),

validation_errors as (

    select
      {{ origin_column }}
    from validation
    where match_value is null

)

select *
from validation_errors

{% endtest %}
