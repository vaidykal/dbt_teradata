with source as (

    select top 100000 * from {{ source('tpch', 'lineitem') }}

),
/*
Had to grant following:
grant execute function on demo_user to dbtdev_vkalpathy with grant option;
grant select on demo_user to dbtdev_vkalpathy with grant option;

Also had to create the hash_md5 function using bteq in demo_user and use it as below

*/
renamed as (

    select
    /*
        {{ dbt_utils.generate_surrogate_key(
            ['l_orderkey', 
            'l_linenumber']) }}
        For the clearscape TD env, demo_user does not have CREATE FUNCTION permission on GLOBAL_FUNCTIONS database
        Hence could not complete the instructions provided here for creating the surrogate_key macro:
        https://github.com/Teradata/dbt-teradata-utils
    */
        cast(hash_md5(cast(coalesce(l_orderkey, '_dbt_utils_surrogate_key_null_') || '-' || coalesce(l_linenumber, '_dbt_utils_surrogate_key_null_') AS varchar(100))) as varchar(100))
                as order_item_key,
        l_orderkey as order_key,
        l_partkey as part_key,
        l_suppkey as supplier_key,
        l_linenumber as line_number,
        cast(l_quantity as bigint) as quantity,
        cast(l_extendedprice as decimal(25,4)) as extended_price,
        cast(l_discount as decimal(25,4)) as discount_percentage,
        cast(l_tax as decimal(25,4)) as tax_rate,
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