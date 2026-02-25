# NYC Taxi Industrial-Scale Data Pipeline 🚕💨

A production-grade ELT pipeline orchestrating high-volume data ingestion from NYC Open Data into a Snowflake Medallion Architecture using Airflow, AWS Glue (Spark), and dbt.

## 🏗️ Architecture

<img width="1920" height="1080" alt="Screenshot (174)" src="https://github.com/user-attachments/assets/e47eddd5-1e08-4a60-9582-fa5a89cb47ff" />

* **Ingestion:** AWS Glue (PySpark) handles parallelized extraction of Parquet files from S3 to a Snowflake Landing Zone.
* **Transformation:** Medallion Architecture (Bronze -> Silver -> Gold) managed via dbt.
* **Orchestration:** Airflow (Dockerized on EC2) with custom Slack observability.
* **Environment Governance:** Automated Zero-Copy Cloning from `PROD` to `DEV` for safe testing.
* **Data Discovery:** Automated dbt Documentation hosted on S3.

## 🚀 Key Features
* **Scalable Backfilling:** Custom Jinja-templated Airflow Params to backfill specific years (2009-2024) on-demand.
* **Zero-Copy Cloning:** Snowflake `CLONE` operations to maintain environment parity without extra storage costs.
* **Observability:** Integrated Slack Webhooks for real-time failure alerting and success notifications.
* **Data Contracts:** Automated schema testing and source freshness checks via dbt.

## 📊 Live Documentation
View the full data lineage and metadata here: http://raghav-saboo-dbt-docs.s3-website.ap-south-1.amazonaws.com
