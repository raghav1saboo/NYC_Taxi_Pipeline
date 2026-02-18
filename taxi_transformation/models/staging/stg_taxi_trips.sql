{{ config(materialized='incremental', unique_key='TRIP_ID', schema='SILVER', on_schema_change='fail') }}

with raw_source as (
    select
        value:vendorid::int as VENDOR_ID,
        value:tpep_pickup_datetime::timestamp as PICKUP_DATETIME,
        value:tpep_dropoff_datetime::timestamp as DROPOFF_DATETIME,
        value:pulocationid::int as PICKUP_LOCATION_ID,
        value:dolocationid::int as DROPOFF_LOCATION_ID,
        value:fare_amount::float as FARE_AMOUNT,
        value:tip_amount::float as TIP_AMOUNT,
        -- Generate surrogate key here to use it for deduplication
        {{ dbt_utils.generate_surrogate_key(['value:vendorid::int', 'value:tpep_pickup_datetime::timestamp']) }} as TRIP_ID
    from {{ source('s3_bronze', 'yellow_taxi_trips') }}
    
    {% if is_incremental() %}
    -- Only pull data newer than what we already have
    where value:tpep_pickup_datetime::timestamp > (select max(PICKUP_DATETIME) from {{ this }})
    {% endif %}
),

deduplicated as (
    -- Handle cases where the source file itself has duplicate entries
    select *,
           row_number() over (partition by TRIP_ID order by PICKUP_DATETIME desc) as rn
    from raw_source
)

select
    TRIP_ID,
    {{ format_currency('FARE_AMOUNT') }} as FARE_USD,
    TIP_AMOUNT,
    PICKUP_DATETIME,
    DROPOFF_DATETIME,
    PICKUP_LOCATION_ID,
    DROPOFF_LOCATION_ID,
    hour(PICKUP_DATETIME) as PICKUP_HOUR,
    dayname(PICKUP_DATETIME) as DAY_OF_WEEK
from deduplicated
where rn = 1;
