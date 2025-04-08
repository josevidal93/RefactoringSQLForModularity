with  source as (

    select 
        id as order_id,
        status as order_status,
        user_id as customer_id
        , order_date
    from {{ source('jaffle_shop', 'orders') }}
    {{ limit_data_in_dev('order_date',30,'2018-04-09T00:00:00') }}


)
, transform as (
select 
row_number() over (partition by customer_id order by order_date, order_id) as user_order_seq,
*,
case 
    when   order_status NOT IN ('returned','return_pending') then order_date
end orders_valid_date,
order_status != 'returned' non_returned
from source
)

select * from transform