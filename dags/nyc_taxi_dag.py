from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

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
    dag_id='nyc_taxi_industrial_pipeline_v1',
    max_active_runs=1,      # Only one year processes at a time
    concurrency=2,
    default_args=default_args,
    description='Industrial scale ingestion (Glue/Spark) and transformation (dbt/Snowflake)',
    schedule_interval='0 12 * * *', # Daily at 12:00 PM
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['production', 'industrial_scale']
) as dag:

    # --- 3. INGESTION: Trigger Parallel Glue Job ---
    # This sends 1GB worth of data (specific years/months) to S3
    ingest_raw_data = GlueJobOperator(
        task_id='trigger_glue_parallel_ingest',
        job_name='nyc_taxi_parallel_ingest_job',
	iam_role_name='DataEngineeringConductorRole',
        script_location='s3://raghav-saboo-ecommerce-lakehouse/scripts/industrial_parallel_ingest.py',
        script_args={
            '--BUCKET_NAME': 'raghav-saboo-ecommerce-lakehouse',
            # Logic: Use manual config if provided, otherwise use current execution year
            '--YEAR_TO_PROCESS': "{{ dag_run.conf['year_to_process'] if dag_run and dag_run.conf.get('year_to_process') else ds[:4] }}",
        },
        aws_conn_id='aws_default'
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
        # Success alert for the final business tables
        on_success_callback=[send_slack_notification(
            slack_conn_id="slack_conn",
            text=":white_check_mark: *dbt Build Success!* Production tables updated in Snowflake."
        )]
    )

    # --- 5. DEPENDENCIES ---
    ingest_raw_data >> run_dbt_transformations
