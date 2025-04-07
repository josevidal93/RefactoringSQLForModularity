with orders as 
    ( 
        select * from {{ ref("stg_jaffle_shop__orders")}} 
        where  order_status NOT IN ('pending') 

    )
 
    , payments as (
        select * from {{ref("stg_stripe__payments")}}
        where payment_status != 'fail'

    ),

    order_totals as 
    (
        select 
            order_id,
            payment_status,
            sum(payment_amount) as order_value_dollars
            
        from payments
        group by order_id, payment_status     
    ) 

 
    select orders.*
    , order_totals.payment_status
    , order_totals.order_value_dollars
    from orders 
    left join order_totals on orders.order_id = order_totals.order_id

