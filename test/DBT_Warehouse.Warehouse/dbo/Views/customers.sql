-- Auto Generated (Do not modify) 33EADB58BDD11D40E9DEA3AA4FF6607106903BE4BF31652106F8DF29E985C5E6
create view "dbo"."customers" as with customers as (

    select * from "DBT_Warehouse"."dbo"."stg_customers"

),

orders as (

    select * from "DBT_Warehouse"."dbo"."stg_orders"

),

payments as (

    select * from "DBT_Warehouse"."dbo"."stg_payments"

),

customer_orders as (

        select
        customer_id,

        min(order_date) as first_order,
        max(order_date) as most_recent_order,
        count(order_id) as number_of_orders
    from orders

    group by customer_id

),

customer_payments as (

    select
        orders.customer_id,
        sum(amount) as total_amount

    from payments

    left join orders on
         payments.order_id = orders.order_id

    group by orders.customer_id

),

final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order,
        customer_orders.most_recent_order,
        customer_orders.number_of_orders,
        customer_payments.total_amount as customer_lifetime_value

    from customers

    left join customer_orders
        on customers.customer_id = customer_orders.customer_id

    left join customer_payments
        on  customers.customer_id = customer_payments.customer_id

)

select * from final;