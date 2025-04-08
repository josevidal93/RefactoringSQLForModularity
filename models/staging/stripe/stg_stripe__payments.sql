with  source as (

    select  id as payment_id,
            orderid as order_id,
            status as payment_status,
            {{ cent_to_dollar('amount',5) }} as payment_amount,
            payment_date,
            payment_method

    from {{ source('stripe', 'payment') }}
    {{ limit_data_in_dev('payment_date',30,'2018-04-09T00:00:00') }}

)
, transform as (
select * from source
) 
select * from transform


