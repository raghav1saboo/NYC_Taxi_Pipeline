import pandas as pd

def model(dbt, session):
    # Setup requirements
    dbt.config(materialized="table", packages=["pandas"])
    
    # Load the intermediate table
    df = dbt.ref("int_taxi_enriched").to_pandas()
    
    # Simple Outlier Analysis: Mark trips with Fare > 100 USD
    df['is_outlier'] = df['FARE_USD'] > 100
    
    return session.create_dataframe(df)
