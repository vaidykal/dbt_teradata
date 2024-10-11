
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with source1 as (
    select 1 as id
),
source2 as (
    select null as id
)
select id from source1
union all
select id from source2
/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
