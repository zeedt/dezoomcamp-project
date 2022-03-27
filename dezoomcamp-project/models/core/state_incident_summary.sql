{{ config(materialized='table') }}


with incidents_summary as (

    select * from {{ref('stg_incidents')}}

)

select state, 
count(*) as number_of_incidents, 
sum(total_offender) as offender_count,
sum(total_victim) as victim_count,
sum(total_offense) as offfence_count,
 from incidents_summary group by 1