-- models/staging/stg_tpch_orders.sql
-- Staging layer: rename columns to snake_case, cast types, add derived fields.
-- Materialized as a VIEW — no storage cost, always fresh.

{{ config(materialized='view') }}

with source as (
    select * from {{ source('tpch', 'orders') }}
),

renamed as (
    select
        -- primary key
        o_orderkey          as order_key,

        -- foreign keys
        o_custkey           as customer_key,

        -- attributes
        o_orderstatus       as status_code,
        o_totalprice        as total_price,
        o_orderdate         as order_date,
        o_orderpriority     as order_priority,
        o_clerk             as clerk_name,
        o_shippriority      as ship_priority,
        o_comment           as order_comment,

        -- derived: extract year and month for easy aggregation
        date_trunc('month', o_orderdate)  as order_month,
        year(o_orderdate)                 as order_year,
        month(o_orderdate)                as order_month_num

    from source
)

select * from renamed
