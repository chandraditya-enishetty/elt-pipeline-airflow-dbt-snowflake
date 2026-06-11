-- models/staging/stg_tpch_lineitems.sql
-- Staging: rename, cast, and add a surrogate key using dbt_utils.

{{ config(materialized='view') }}

with source as (
    select * from {{ source('tpch', 'lineitem') }}
),

renamed as (
    select
        -- surrogate key (lineitem has no single PK — it's a composite)
        {{ dbt_utils.generate_surrogate_key(['l_orderkey', 'l_linenumber']) }}
                                    as order_item_key,

        -- foreign keys
        l_orderkey                  as order_key,
        l_partkey                   as part_key,
        l_suppkey                   as supplier_key,

        -- attributes
        l_linenumber                as line_number,
        l_quantity                  as quantity,
        l_extendedprice             as extended_price,
        l_discount                  as discount_percentage,
        l_tax                       as tax_rate,
        l_returnflag                as return_flag,
        l_linestatus                as line_status,
        l_shipdate                  as ship_date,
        l_commitdate                as commit_date,
        l_receiptdate               as receipt_date,
        l_shipinstruct              as ship_instructions,
        l_shipmode                  as ship_mode,
        l_comment                   as line_comment,

        -- derived: gross revenue before discount
        l_extendedprice * (1 - l_discount) as net_price,

        -- derived: days between commit date and ship date (positive = late)
        datediff('day', l_commitdate, l_shipdate) as days_to_ship_vs_commit

    from source
)

select * from renamed
