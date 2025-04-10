-- with statement
-- import CTEs

with orders as 
    ( 
        select * from {{ ref("int_orders")}} 
    )
    , customers as (
        select * from {{ ref("stg_jaffle_shop__customers")}}
    )
    , payments as (
        select * from {{ref("stg_stripe__payments")}}
        where payment_status != 'fail'

    ),

    --NVL2(expression, value_if_not_null, value_if_null)

    
    customer_orders  as 
    (

        select orders.*,
        min(order_date) over ( partition by orders.customer_id) as first_order_date,
        min(orders_valid_date) over ( partition by orders.customer_id) as first_non_returned_order_date,
        max(orders_valid_date) over ( partition by orders.customer_id) as most_recent_non_returned_order_date,
        COALESCE(
            count(order_id) over ( partition by orders.customer_id),0
            ) as order_count,
        COALESCE(
            count(
                case 
                    when  non_returned then 1 
                end
            ) over ( partition by orders.customer_id)
        ,0) as non_returned_order_count,
        sum( nvl2(orders_valid_date, ROUND(order_value_dollars,2) , 0) ) over ( partition by orders.customer_id) as total_lifetime_value,
        sum( nvl2(orders_valid_date, 1 , 0) ) over ( partition by orders.customer_id) as non_returned_orders
        ,array_agg(distinct customers.customer_id ) over ( partition by orders.customer_id) as order_ids         
        from 
        orders inner join customers on orders.customer_id   = customers.customer_id
    )
     ,customer_orders_avg as (

        select *,nvl2(orders_valid_date,total_lifetime_value/order_count,null) as avg_non_returned_order_value from customer_orders
    )

-- final CTE
, final as (
select 
    customer_orders_avg.order_id,
    customer_orders_avg.customer_id,
    customers.surname,
    customers.givenname,
    first_order_date,
    order_count,
    total_lifetime_value,
    customer_orders_avg.order_value_dollars,
    avg_non_returned_order_value,
    customer_orders_avg.order_status,
    customer_orders_avg.payment_status
from   customer_orders_avg

join   customers
on customer_orders_avg.customer_id = customers.customer_id


)
-- simple select statement

select * from final
