CREATE DATABASE TAXI_GOLD;
CREATE SCHEMA TAXI_GOLD.ANALYTICS;
CREATE WAREHOUSE TAXI_WH WITH WAREHOUSE_SIZE = 'XSMALL';

CREATE OR REPLACE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('s3://raghav-saboo-ecommerce-lakehouse/bronze/')
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::480957639219:role/DataEngineeringConductorRole';

-- DESCRIBE the integration to get the SNOWFLAKE_IAM_USER_ARN and EXTERNAL_ID
-- You MUST add these to your AWS Role's 'Trust Relationship'
DESC INTEGRATION s3_int;

CREATE OR REPLACE FILE FORMAT parquet_format TYPE = PARQUET;

CREATE OR REPLACE STAGE s3_bronze_stage
  URL = 's3://raghav-saboo-ecommerce-lakehouse/bronze/'
  STORAGE_INTEGRATION = s3_int
  FILE_FORMAT = parquet_format;

-- This is the "Source" dbt will look for
CREATE OR REPLACE EXTERNAL TABLE TAXI_GOLD.ANALYTICS.yellow_taxi_trips
  LOCATION = @s3_bronze_stage
  FILE_FORMAT = parquet_format
  AUTO_REFRESH = true;

-- 1. Create the Production Database (The Source of Truth)
CREATE DATABASE IF NOT EXISTS TAXI_PROD;

-- 2. Create the Development/Testing Database 
-- In 2026, we use CLONE to instantly copy Prod structure without extra cost
CREATE DATABASE TAXI_DEV CLONE TAXI_PROD;

-- 3. Create Roles for Governance
CREATE ROLE IF NOT EXISTS DE_PROD_ROLE;
CREATE ROLE IF NOT EXISTS DE_DEV_ROLE;

-- 4. Grant Permissions
GRANT ALL ON DATABASE TAXI_PROD TO ROLE DE_PROD_ROLE;
GRANT ALL ON DATABASE TAXI_DEV TO ROLE DE_DEV_ROLE; 

CREATE SCHEMA IF NOT EXISTS TAXI_PROD.BRONZE; -- Raw/External tables
CREATE SCHEMA IF NOT EXISTS TAXI_PROD.SILVER; -- Cleaned/Transformed (dbt)
CREATE SCHEMA IF NOT EXISTS TAXI_PROD.GOLD;   -- Final Analytics (dbt)

CREATE SCHEMA IF NOT EXISTS TAXI_DEV.BRONZE; -- Raw/External tables
CREATE SCHEMA IF NOT EXISTS TAXI_DEV.SILVER; -- Cleaned/Transformed (dbt)
CREATE SCHEMA IF NOT EXISTS TAXI_DEV.GOLD;   -- Final Analytics (DEV

-- 3. Move/Recreate your External Table in the BRONZE layer
-- This makes logical sense: Raw S3 data lives in 'Bronze'
CREATE OR REPLACE EXTERNAL TABLE TAXI_PROD.BRONZE.yellow_taxi_trips
  LOCATION = @TAXI_GOLD.ANALYTICS.S3_BRONZE_STAGE -- Use your existing stage
  FILE_FORMAT = (TYPE = PARQUET)
  AUTO_REFRESH = true;

CREATE OR REPLACE EXTERNAL TABLE TAXI_DEV.BRONZE.yellow_taxi_trips
  LOCATION = @TAXI_GOLD.ANALYTICS.S3_BRONZE_STAGE -- Use your existing stage
  FILE_FORMAT = (TYPE = PARQUET)
  AUTO_REFRESH = true;

SELECT * FROM TAXI_PROD.BRONZE.yellow_taxi_trips LIMIT 10;

-- 1. Grant the role to your user so you can assume it
GRANT ROLE DE_DEV_ROLE TO USER "RAGHAVSABOO";
GRANT ROLE DE_PROD_ROLE TO USER "RAGHAVSABOO";

-- 2. Grant the role to the SYSADMIN (Standard Snowflake Best Practice)
-- This allows the system to manage these roles
GRANT ROLE DE_DEV_ROLE TO ROLE SYSADMIN;
GRANT ROLE DE_PROD_ROLE TO ROLE SYSADMIN;

-- 3. Set the default role for your user (Optional, but helpful for UI)
ALTER USER "RAGHAVSABOO" SET DEFAULT_ROLE = DE_DEV_ROLE;

-- Grant usage on the Warehouse
GRANT USAGE ON WAREHOUSE TAXI_WH TO ROLE DE_DEV_ROLE;
GRANT USAGE ON WAREHOUSE TAXI_WH TO ROLE DE_PROD_ROLE;

-- Grant usage on the Databases
GRANT ALL ON DATABASE TAXI_DEV TO ROLE DE_DEV_ROLE;
GRANT ALL ON DATABASE TAXI_PROD TO ROLE DE_PROD_ROLE;

-- Grant usage on the Schemas
GRANT ALL ON ALL SCHEMAS IN DATABASE TAXI_DEV TO ROLE DE_DEV_ROLE;
GRANT ALL ON ALL SCHEMAS IN DATABASE TAXI_PROD TO ROLE DE_PROD_ROLE;

DROP SCHEMA IF EXISTS TAXI_DEV.ANALYTICS CASCADE;

select * from snapshots.snp_taxi_zones limit 10;

DROP TABLE IF EXISTS TAXI_DEV.SILVER.stg_taxi_trips CASCADE;

DROP SCHEMA IF EXISTS TAXI_PROD.SILVER CASCADE;
DROP SCHEMA IF EXISTS TAXI_DEV.GOLD CASCADE;

select * from GOLD.fct_trip_analysis limit 10;

select count(*) from silver.stg_taxi_trips limit 10;

select *,count(*) as rows_cnt from silver.taxi_zone_lookup group by all ;