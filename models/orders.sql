with orders as (
  select order_id,
  customer_id,
  order_date,
  status
  from {{ ref('stg_orders') }}
),
success_payments as (
  select order_id
  ,sum(amount) amount
  from {{ ref('stg_payment') }}
  where status = 'success'
  group by order_id
),
final as
(
  select ord.ORDER_ID
  ,ord.customer_id
  , pay.amount
  from orders ord
  left outer join success_payments pay
  ON ord.order_id = pay.order_id
)
select * from final
