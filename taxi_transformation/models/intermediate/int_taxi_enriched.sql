{{ config(materialized='incremental', unique_key='TRIP_ID', schema='SILVER') }}

select
    t.*,
    -- Handle missing zones by assigning 'Unknown' instead of NULL
    coalesce(z.BOROUGH, 'Unknown') as PICKUP_BOROUGH,
    coalesce(z.ZONE, 'Unknown') as PICKUP_ZONE,
    timestampdiff(minute, t.PICKUP_DATETIME, t.DROPOFF_DATETIME) as DURATION_MINUTES
from {{ ref('stg_taxi_trips') }} t
left join {{ ref('snp_taxi_zones') }} z 
    on t.PICKUP_LOCATION_ID = z.LOCATIONID
where z.DBT_VALID_TO is null
