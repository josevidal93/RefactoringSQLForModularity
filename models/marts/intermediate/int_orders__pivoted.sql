/*with payments as (
    select * from {{ ref("stg_stripe__payments")}}
)
, pivoted as (
    select order_id,
            sum( case when payment_method = 'bank_transfer' then payment_amount else 0 end ) bank_transfer_amount,
            sum( case when payment_method = 'coupon' then payment_amount else 0 end ) coupon_amount,
            sum( case when payment_method = 'credit_card' then payment_amount else 0 end ) credit_card_amount ,
            sum( case when payment_method = 'gift_card' then payment_amount else 0 end ) gift_card_amount 


    from payments 
    where payment_status = 'success'
    group by 1
)
select  * from pivoted
*/



with payments as (
    select * from {{ ref("stg_stripe__payments")}}
)

{% set payment_methods = ['bank_transfer','coupon','credit_card','gift_card'] %}


SELECT order_id,
 {% for payment_method in payment_methods -%}
   sum( case when payment_method = '{{ payment_method }}' then  payment_amount else 0 end ) as {{ payment_method }}_amount 
   {%- if not loop.last  -%} 
     , 
   {% endif %}
 {% endfor %}
 FROM payments
 where payment_status = 'success'
 GROUP BY 1