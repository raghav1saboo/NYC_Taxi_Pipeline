import pandas as pd

def model(dbt, session):
    dbt.config(materialized="table", schema="GOLD", packages=["pandas"])
    
    # Snowpark to Pandas converts column names to UPPERCASE by default
    df = dbt.ref("int_taxi_enriched").to_pandas()
    
    # Fixed Logic: Access correct uppercase column names
    df['NET_PROFIT'] = df['FARE_USD'] * 0.85 
    df['IS_HIGH_VALUE'] = (df['FARE_USD'] > 50) & (df['FARE_USD'] < 500)
    
    return session.create_dataframe(df)
