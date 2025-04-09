with payments as 
(
    select * from {{ ref('stg_stripe__payments')}}
)
, orders as (
    select * from {{ ref("stg_jaffle_shop__orders")}}
)
, employees as (
        select * from {{ ref('seed_employees')}}

)

select employees.employee_id, year(payment_date) year, month(payment_date) month, sum(payment_amount) total_amount
from payments 
inner join orders on orders.order_id = payments.order_id 
inner join  employees on employees.customer_id = orders.customer_id
where payment_status = 'success'
group by 1,2,3



 