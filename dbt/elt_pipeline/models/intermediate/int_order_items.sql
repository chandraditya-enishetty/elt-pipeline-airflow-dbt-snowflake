-- models/intermediate/int_order_items.sql
-- Intermediate layer: join orders and line items, apply business logic.
-- This is where the macro for discounted_amount is used.

{{ config(materialized='view') }}

with orders as (
    select * from {{ ref('stg_tpch_orders') }}
),

line_items as (
    select * from {{ ref('stg_tpch_lineitems') }}
),

joined as (
    select
        -- identifiers
        li.order_item_key,
        li.order_key,
        li.part_key,
        li.supplier_key,
        li.line_number,

        -- order attributes
        o.customer_key,
        o.status_code       as order_status,
        o.order_date,
        o.order_month,
        o.order_year,
        o.order_priority,

        -- line item attributes
        li.quantity,
        li.extended_price,
        li.discount_percentage,
        li.tax_rate,
        li.return_flag,
        li.ship_date,
        li.ship_mode,
        li.days_to_ship_vs_commit,

        -- financial calculations using the macro
        {{ discounted_amount('li.extended_price', 'li.discount_percentage') }}
            as discount_amount,

        li.extended_price * (1 - li.discount_percentage)
            as net_item_price,

        li.extended_price * (1 - li.discount_percentage) * (1 + li.tax_rate)
            as gross_item_price,

        -- derived: shipping performance flag
        case
            when li.days_to_ship_vs_commit <= 0  then 'on_time'
            when li.days_to_ship_vs_commit <= 3  then 'slightly_late'
            else                                      'late'
        end as shipping_performance

    from line_items li
    inner join orders o
        on li.order_key = o.order_key
)

select * from joined
