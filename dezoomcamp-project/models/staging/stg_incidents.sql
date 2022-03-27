{{ config(materialized='view') }}

with incidents as (

    select * from {{ source('staging','incidents_partitioned_clustered') }}

)

select * from incidents

{% if (var('is_test_run')) %}
    limit 100
{% endif %}