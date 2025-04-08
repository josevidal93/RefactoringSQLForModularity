with  source as (

    select  id as payment_id,
            orderid as order_id,
            status as payment_status,
            {{ cent_to_dollar('amount',5) }} as payment_amount,
            payment_date,
            payment_method

    from {{ source('stripe', 'payment') }}

)
, transform as (
select * from source
) 
select * from transform


