from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
import os
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json
import boto3

def send_slack_webhook_alert(context):
    ti = context.get('task_instance')
    msg = f":rotating_light: *Task Failed!* \n*Task*: {ti.task_id} \n*DAG*: {ti.dag_id}"
    
    # This hook specifically handles the hooks.slack.com/services/... format
    hook = SlackWebhookHook(slack_webhook_conn_id='slack_conn')
    return hook.send(text=msg)


# --- 1. OBSERVABILITY: Slack Alerting ---
# This notifier will trigger on any task failure
failure_alert = send_slack_notification(
    slack_conn_id="slack_conn",
    text=(
        ":rotating_light: *Industrial Pipeline Task Failed!*\n"
        "*DAG*: {{ dag.dag_id }}\n"
        "*Task*: {{ task_instance.task_id }}\n"
        "*Execution Time*: {{ ts }}\n"
        "*Logs*: {{ task_instance.log_url }}"
    )
)

# --- 2. DAG DEFINITION ---
default_args = {
    'owner': 'raghav_saboo',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': send_slack_webhook_alert # Global failure alerting
}

with DAG(
    dag_id='nyc_taxi_pipeline_v1',
    max_active_runs=1,      # Only one year processes at a time
    concurrency=1,
    default_args=default_args,
    description='Scaled ingestion (Glue/Spark) and transformation (dbt/Snowflake)',
    schedule_interval='0 12 * * *', # Daily at 12:00 PM
    start_date=datetime(2026, 2, 23),
    catchup=True,
    params={
        "year_to_process": Param(
            default=None, 
            type=["null", "string"], 
            description="Manual override for year (e.g. 2024). Leave empty for auto-backfill."
        )
    },
    render_template_as_native_obj=True,
    tags=['production', 'industrial_scale']
) as dag:

    # --- 3. INGESTION: Trigger Parallel Glue Job ---
    # This sends 1GB worth of data (specific years/months) to S3

    target_year_expr = """
    {% if params.year_to_process %}
        {{ params.year_to_process }}
    {% else %}
        {{ (dag_run.logical_date - dag.start_date).days + 2009 }}
    {% endif %}
    """

    ingest_raw_data = GlueJobOperator(
        task_id='trigger_glue_parallel_ingest',
        job_name='nyc_taxi_parallel_ingest_job',
	iam_role_name='DataEngineeringConductorRole',
        script_location='s3://raghav-saboo-ecommerce-lakehouse/scripts/industrial_parallel_ingest.py',
	create_job_kwargs={
            "GlueVersion": "4.0",
            "Command": {
                "Name": "glueetl",
                "ScriptLocation": "s3://raghav-saboo-ecommerce-lakehouse/scripts/industrial_parallel_ingest.py",
                "PythonVersion": "3",
            }
        },
        script_args={
            '--BUCKET_NAME': 'raghav-saboo-ecommerce-lakehouse',
            '--YEAR_TO_PROCESS': target_year_expr.strip()
        },
        aws_conn_id='aws_default'
    )

    debug_env_task = BashOperator(
        task_id='debug_snowflake_connection',
        # Wrapping in raw tags tells Jinja to stop looking for comments or variables here
        bash_command='{% raw %}echo "User: $SNOWFLAKE_USER" && echo "Account: $SNOWFLAKE_ACCOUNT" && echo "Password Length: ${#SNOWFLAKE_PASSWORD}"{% endraw %}',
        env={
            'SNOWFLAKE_USER': os.getenv('SNOWFLAKE_USER'),
            'SNOWFLAKE_PASSWORD': os.getenv('SNOWFLAKE_PASSWORD'),
            'SNOWFLAKE_ACCOUNT': os.getenv('SNOWFLAKE_ACCOUNT'),
        },
    )

    # --- 4. TRANSFORMATION: Execute dbt Models ---
    # This runs within your custom Docker container environment
    run_dbt_transformations = BashOperator(
        task_id='dbt_run_and_test',
        bash_command=(
            'cd /opt/airflow/taxi_transformation && '
            'dbt deps && '
            'dbt build --target prod'
        ),
	env={
            # This pulls the password from the .env file (already loaded into the container)
            # and passes it specifically to this bash command
            'SNOWFLAKE_PASSWORD': os.getenv('SNOWFLAKE_PASSWORD'),
            'SNOWFLAKE_USER': os.getenv('SNOWFLAKE_USER'),
            'SNOWFLAKE_ACCOUNT': os.getenv('SNOWFLAKE_ACCOUNT'),
            'PATH': os.environ.get('PATH') # Ensure dbt can find its own path
        },
        append_env=True,
        # Success alert for the final business tables
        on_success_callback=[send_slack_notification(
            slack_conn_id="slack_conn",
            text=":white_check_mark: *dbt Build Success!* Production tables updated in Snowflake."
        )]
    )

    clone_prod_to_dev = BashOperator(
        task_id='clone_dev_from_prod',
        bash_command=(
            'cd /opt/airflow/taxi_transformation && '
            'dbt run-operation clone_schema_across_db '
            '--args "{from_db: TAXI_PROD, from_schema: BRONZE, to_db: TAXI_DEV, to_schema: BRONZE}" '
            '--target clone'
        ),
        env={
            'SNOWFLAKE_USER': os.getenv('SNOWFLAKE_USER'),
            'SNOWFLAKE_PASSWORD': os.getenv('SNOWFLAKE_PASSWORD'),
            'SNOWFLAKE_ACCOUNT': os.getenv('SNOWFLAKE_ACCOUNT'),
        },
        append_env=True
    )

    def upload_dbt_docs_to_s3():
     s3_hook = S3Hook(aws_conn_id='aws_default')
     base_path = '/opt/airflow/taxi_transformation/target'
     bucket_name = 'raghav-saboo-dbt-docs'
     s3_client = s3_hook.get_conn()
    
     files = ['index.html', 'manifest.json', 'catalog.json']
    
     for file in files:
        s3_hook.load_file(
            filename=f"{base_path}/{file}",
            key=file,
            bucket_name=bucket_name,
            replace=True
        )
     with open(f"{base_path}/index.html", "r") as f:
        index = f.read()
     with open(f"{base_path}/manifest.json", "r") as f:
        manifest = json.load(f)
     with open(f"{base_path}/catalog.json", "r") as f:
        catalog = json.load(f)

     # 2. Merge them (Injecting JSON into the HTML)
     # dbt looks for specific search strings to replace with the actual data
     new_index = index.replace('"[search_string_for_manifest]"', json.dumps(manifest))
     new_index = new_index.replace('"[search_string_for_catalog]"', json.dumps(catalog))

     # 3. Save the single merged file
     merged_file_path = f"{base_path}/single_index.html"
     with open(merged_file_path, "w") as f:
        f.write(new_index)

     # 4. Upload ONLY the merged file
     s3_client.put_object(
        Bucket=bucket_name,
        Key='index.html',
        Body=new_index,
        ContentType='text/html' # This is the standard Boto3 argument name
     )
     print(f"Successfully uploaded merged docs to {bucket_name}")

    generate_dbt_docs = BashOperator(
        task_id='generate_dbt_docs',
        bash_command=(
            'cd /opt/airflow/taxi_transformation && dbt docs generate --target prod '
        ),
        env={
            'SNOWFLAKE_USER': os.getenv('SNOWFLAKE_USER'),
            'SNOWFLAKE_PASSWORD': os.getenv('SNOWFLAKE_PASSWORD'),
            'SNOWFLAKE_ACCOUNT': os.getenv('SNOWFLAKE_ACCOUNT'),
        },
        append_env=True
    )

    upload_docs_to_s3 = PythonOperator(
     task_id='upload_docs_to_s3',
     python_callable=upload_dbt_docs_to_s3
    )

    # --- 5. DEPENDENCIES ---
    ingest_raw_data >> debug_env_task >> run_dbt_transformations >> clone_prod_to_dev >> generate_dbt_docs >> upload_docs_to_s3
