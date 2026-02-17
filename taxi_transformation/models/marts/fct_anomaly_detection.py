import pandas as pd

def model(dbt, session):
    dbt.config(materialized="table", schema="gold", packages=["pandas"])
    df = dbt.ref("int_taxi_enriched").to_pandas()
    
    # Anomaly Logic: Fare > $200
    df['IS_ANOMALY'] = df['FARE_USD'] > 200
    return session.create_dataframe(df)
