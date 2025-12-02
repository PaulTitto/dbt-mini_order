{% test is_qty_less_then_zero(model, column_name) %}

with qty as (
    select {{ column_name }} as order_qty
    from {{ model }}
),

check_order_qty as (
    select order_qty
    from qty
    where order_qty < 0
)

select * from check_order_qty

{% endtest %}