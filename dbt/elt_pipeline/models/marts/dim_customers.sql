-- models/marts/dim_customers.sql
-- Dimension table: enriched customer data with order summary stats.
-- Updated daily by Airflow. Used by BI tools to slice fact tables by customer.

{{ config(materialized='table') }}

with customers as (
    select * from {{ ref('stg_tpch_customers') }}
),

orders as (
    select * from {{ ref('stg_tpch_orders') }}
),

-- Aggregate order stats per customer
customer_order_summary as (
    select
        customer_key,
        count(order_key)            as lifetime_order_count,
        sum(total_price)            as lifetime_revenue,
        min(order_date)             as first_order_date,
        max(order_date)             as most_recent_order_date,
        avg(total_price)            as avg_order_value

    from orders
    group by 1
),

final as (
    select
        c.customer_key,
        c.customer_name,
        c.market_segment,
        c.balance_tier,
        c.account_balance,
        c.phone_number,

        -- order stats
        coalesce(cos.lifetime_order_count, 0)   as lifetime_order_count,
        coalesce(cos.lifetime_revenue, 0)       as lifetime_revenue,
        cos.first_order_date,
        cos.most_recent_order_date,
        coalesce(cos.avg_order_value, 0)        as avg_order_value,

        -- derived: customer value tier based on lifetime revenue
        case
            when coalesce(cos.lifetime_revenue, 0) = 0
                then 'no_orders'
            when cos.lifetime_revenue < 100000
                then 'standard'
            when cos.lifetime_revenue < 500000
                then 'valuable'
            else
                'high_value'
        end as customer_value_tier,

        -- days since last order (recency)
        datediff('day', cos.most_recent_order_date, current_date()) as days_since_last_order

    from customers c
    left join customer_order_summary cos
        on c.customer_key = cos.customer_key
)

select * from final
