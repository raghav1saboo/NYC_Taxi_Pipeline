{% snapshot snp_taxi_zones %}
{{
    config(
      target_schema='snapshots',
      strategy='check',
      unique_key='locationid',
      check_cols=['borough', 'zone'],
    )
}}
select * from {{ ref('taxi_zone_lookup') }}
{% endsnapshot %}


