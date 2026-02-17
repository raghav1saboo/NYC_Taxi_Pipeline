FROM apache/airflow:2.7.1-python3.9

USER root
RUN apt-get update && apt-get install -y git && apt-get clean

USER airflow
RUN pip install --no-cache-dir \
    dbt-snowflake \
    boto3 \
    requests \
    apache-airflow-providers-amazon \
    apache-airflow-providers-snowflake \
    apache-airflow-providers-slack
