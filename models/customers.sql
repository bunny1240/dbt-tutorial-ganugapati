with customers as (

  select * from {{ ref('stg_customers') }}

),

orders as (

  select * from {{ ref('stg_orders') }}

),

customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders

    from orders

    group by 1

),
customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders

    from orders

    group by 1

),
customer_payment as (

    select
        co.customer_id,
        sum(ord.amount) lifetime_value
    from orders co
    inner join {{ ref('orders') }} ord on co.order_id = ord.order_id
    group by 1

),

final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        coalesce(cp.lifetime_value, 0) lifetime_value
    from customers

    left join customer_orders using (customer_id)
    left join customer_payment cp on cp.customer_id = customers.customer_id

)

select * from final
