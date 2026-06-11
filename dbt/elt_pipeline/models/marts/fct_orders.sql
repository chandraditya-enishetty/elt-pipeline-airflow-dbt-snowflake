


{{ config(
    materialized='table',
    cluster_by=['order_date']
) }}

with order_items as (
    select * from {{ ref('int_order_items') }}
),

customers as (
    select * from {{ ref('stg_tpch_customers') }}
),

order_summary as (
    select
        -- grain: one row per order
        order_key,
        customer_key,
        order_status,
        order_date,
        order_month,
        order_year,
        order_priority,

        -- aggregated line item metrics
        count(order_item_key)                   as num_line_items,
        sum(quantity)                           as total_quantity,
        sum(extended_price)                     as gross_revenue,
        sum(discount_amount)                    as total_discount,
        sum(net_item_price)                     as net_revenue,
        sum(gross_item_price)                   as total_revenue_with_tax,
        avg(discount_percentage)                as avg_discount_pct,

        -- shipping performance metrics
        count_if(shipping_performance = 'on_time')       as items_on_time,
        count_if(shipping_performance = 'slightly_late') as items_slightly_late,
        count_if(shipping_performance = 'late')          as items_late,

        -- return flag
        count_if(return_flag = 'R')             as returned_items,

        -- derived: order-level discount rate
        round(
            sum(discount_amount) / nullif(sum(extended_price), 0) * 100,
            2
        ) as order_discount_pct

    from order_items
    group by 1, 2, 3, 4, 5, 6, 7
)

-- Final select — join with customer for market segment
select
    os.*,
    c.customer_name,
    c.market_segment,
    c.balance_tier,
    c.account_balance

from order_summary os
left join customers c
    on os.customer_key = c.customer_key
