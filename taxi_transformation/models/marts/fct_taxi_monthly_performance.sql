{{ config(materialized='table', schema='GOLD') }}

with enriched_trips as (
    select * from {{ ref('int_taxi_enriched') }}
),
anomalies as (
    select TRIP_ID, IS_ANOMALY from {{ ref('fct_anomaly_detection') }}
)

select
    date_trunc('month', t.PICKUP_DATETIME) as REPORT_MONTH,
    t.PICKUP_BOROUGH,
    count(t.TRIP_ID) as TOTAL_TRIPS,
    sum(t.FARE_USD) as TOTAL_REVENUE,
    -- Now we use IS_ANOMALY (renamed from IS_OUTLIER for consistency)
    sum(case when a.IS_ANOMALY = true then 1 else 0 end) as TOTAL_ANOMALIES
from enriched_trips t
left join anomalies a on t.TRIP_ID = a.TRIP_ID
group by 1, 2
