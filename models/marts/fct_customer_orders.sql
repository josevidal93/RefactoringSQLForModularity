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
    )/*,

    customer_order_history as (

    select 
        customers.customer_id,
        customers.full_name,
        customers.surname,
        customers.givenname,
        min(order_date) as first_order_date,
        min(orders_valid_date) as first_non_returned_order_date,
        max(orders_valid_date) as most_recent_non_returned_order_date,
        COALESCE(
            max(user_order_seq),0
            ) as order_count,
        COALESCE(
            count(
                case 
                    when  non_returned then 1 
                end
            )
        ,0) as non_returned_order_count,
        sum(
            case 
                when  orders_valid_date is not null then ROUND(payments.payment_amount/100.0,2) 
                    else 0 
            end) as total_lifetime_value,
        sum(
            case 
                when  orders_valid_date is not null then ROUND(payments.payment_amount/100.0,2) 
                    else 0 end)
            /NULLIF(
                    count(
                        case 
                            when  orders_valid_date is not null then 1 end
                        )
                ,0) as avg_non_returned_order_value,
        array_agg(distinct customers.customer_id) as order_ids

    from orders

    join customers
    on orders.customer_id = customers.customer_id

    left outer join  payments
    on orders.order_id = payments.order_id


    group by  customers.customer_id, customers.full_name, customers.surname, customers.givenname


)*/

-- final CTE
, final as (
select 
    orders.order_id,
    orders.customer_id,
    customers.surname,
    customers.givenname,
    first_order_date,
    order_count,
    total_lifetime_value,
    orders.order_value_dollars,
    avg_non_returned_order_value,
    orders.order_status,
    orders.payment_status
from   orders

join   customers
on orders.customer_id = customers.customer_id

join   customer_orders_avg
on orders.customer_id = customer_orders_avg.customer_id

)
-- simple select statement

select * from final
