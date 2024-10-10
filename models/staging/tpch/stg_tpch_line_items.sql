with source as (

    select * from {{ source('tpch', 'lineitem') }}

),

renamed as (

    select
        hash_md5(cast(coalesce(l_orderkey, '_dbt_utils_surrogate_key_null_') || '-' || coalesce(l_linenumber, '_dbt_utils_surrogate_key_null_') AS LONG VARCHAR))
                as order_item_key,
        l_orderkey as order_key,
        l_partkey as part_key,
        l_suppkey as supplier_key,
        l_linenumber as line_number,
        l_quantity as quantity,
        l_extendedprice as extended_price,
        l_discount as discount_percentage,
        l_tax as tax_rate,
        l_returnflag as return_flag,
        l_linestatus as status_code,
        l_shipdate as ship_date,
        l_commitdate as commit_date,
        l_receiptdate as receipt_date,
        l_shipinstruct as ship_instructions,
        l_shipmode as ship_mode,
        l_comment as l_comment

    from source

)

select * from renamed