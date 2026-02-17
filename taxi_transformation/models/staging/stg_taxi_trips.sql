{{ config(materialized='incremental', unique_key='TRIP_ID', schema='SILVER') }}

with raw_source as (
    select
        value:vendorid::int as VENDOR_ID,
        value:tpep_pickup_datetime::timestamp as PICKUP_DATETIME,
        value:tpep_dropoff_datetime::timestamp as DROPOFF_DATETIME,
        value:pulocationid::int as PICKUP_LOCATION_ID,
        value:dolocationid::int as DROPOFF_LOCATION_ID,
        value:fare_amount::float as FARE_AMOUNT,
        value:tip_amount::float as TIP_AMOUNT
    from {{ source('s3_bronze', 'yellow_taxi_trips') }}
)
select
    {{ dbt_utils.generate_surrogate_key(['VENDOR_ID', 'PICKUP_DATETIME']) }} as TRIP_ID,
    {{ format_currency('FARE_AMOUNT') }} as FARE_USD,
    TIP_AMOUNT,
    PICKUP_DATETIME,
    DROPOFF_DATETIME,
    PICKUP_LOCATION_ID,
    DROPOFF_LOCATION_ID,
    hour(PICKUP_DATETIME) as PICKUP_HOUR,
    dayname(PICKUP_DATETIME) as DAY_OF_WEEK
from raw_source
{% if is_incremental() %}
  where PICKUP_DATETIME > (select max(PICKUP_DATETIME) from {{ this }})
{% endif %}
