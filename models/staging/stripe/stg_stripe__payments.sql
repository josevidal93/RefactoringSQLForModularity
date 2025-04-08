with  source as (

    select  orderid as order_id,
            status as payment_status,
            amount as payment_amount,
            payment_date,
            payment_method

    from {{ source('stripe', 'payment') }}

)
, transform as (
select * from source
) 
select * from transform


