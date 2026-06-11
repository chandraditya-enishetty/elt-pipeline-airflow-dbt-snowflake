-- models/staging/stg_tpch_customers.sql

{{ config(materialized='view') }}

with source as (
    select * from {{ source('tpch', 'customer') }}
),

renamed as (
    select
        c_custkey       as customer_key,
        c_name          as customer_name,
        c_address       as customer_address,
        c_nationkey     as nation_key,
        c_phone         as phone_number,
        c_acctbal       as account_balance,
        c_mktsegment    as market_segment,
        c_comment       as customer_comment,

        -- derived: classify customers by account balance
        case
            when c_acctbal < 0       then 'negative'
            when c_acctbal < 1000    then 'low'
            when c_acctbal < 5000    then 'medium'
            else                          'high'
        end as balance_tier

    from source
)

select * from renamed
