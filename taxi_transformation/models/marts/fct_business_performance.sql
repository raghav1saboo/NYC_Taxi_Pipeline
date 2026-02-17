{{ config(materialized='table', schema='GOLD') }}

select
    date_trunc('month', PICKUP_DATETIME) as REPORT_MONTH,
    PICKUP_BOROUGH,
    count(TRIP_ID) as TOTAL_TRIPS,
    sum(FARE_USD) as TOTAL_REVENUE,
    sum(TIP_AMOUNT) as TOTAL_TIPS,
    round(sum(FARE_USD) / count(TRIP_ID), 2) as AVG_REVENUE_PER_TRIP
from {{ ref('int_taxi_enriched') }}
group by 1, 2
