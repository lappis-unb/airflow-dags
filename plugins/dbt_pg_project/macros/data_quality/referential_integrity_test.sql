{% test referential_integrity_test(model, column_name, parent_model, parent_column, filter=none, parent_filter=none) %}


with parent as (
    select distinct {{ parent_column }}
    from {{ ref(parent_model) }}
    {% if parent_filter %}
    where {{ parent_filter }}
    {% endif %}
),
child as (
    select *
    from {{ model }}
    {% if filter %}
    where {{ filter }}
    {% endif %}
),
invalid_references as (
    select child.*
    from child
    left join parent on child.{{ column_name }} = parent.{{ parent_column }}
    where parent.{{ parent_column }} is null
)

select *
from invalid_references

{% endtest %}
