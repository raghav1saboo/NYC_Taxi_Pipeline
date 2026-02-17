{{ config(materialized='table', schema='GOLD') }}

select
    PICKUP_HOUR,
    DAY_OF_WEEK,
    PICKUP_BOROUGH,
    PICKUP_ZONE,
    count(TRIP_ID) as TRIP_COUNT
from {{ ref('int_taxi_enriched') }}
group by 1, 2, 3, 4
