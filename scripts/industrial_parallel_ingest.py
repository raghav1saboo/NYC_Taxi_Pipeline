import sys
import requests
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from botocore.exceptions import ClientError

# Capture Runtime Arguments
args = getResolvedOptions(sys.argv, ['BUCKET_NAME', 'YEAR_TO_PROCESS'])
DEST_BUCKET = args['BUCKET_NAME']
YEAR = args['YEAR_TO_PROCESS']
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

# Initialize Contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define All Data Types in NYC Dataset
TAXI_TYPES = ['yellow', 'green', 'fhv', 'fhvhv']

# Create a list of all combinations (Type x Month)
# Total: 4 types * 12 months = 48 tasks
tasks = [(t, m) for t in TAXI_TYPES for m in range(1, 13)]
tasks_rdd = sc.parallelize(tasks, numSlices=48)

def ingest_data_type(task):
    taxi_type, month = task
    s3 = boto3.client('s3')
    
    # Construct Filename based on NYC naming convention
    # Example: yellow_tripdata_2025-01.parquet
    file_name = f"{taxi_type}_tripdata_{YEAR}-{month:02d}.parquet"
    s3_key = f"taxi_data/type={taxi_type}/year={YEAR}/month={month:02d}/{file_name}"
    url = f"{BASE_URL}{file_name}"

    # Idempotency Check
    try:
        s3.head_object(Bucket=DEST_BUCKET, Key=s3_key)
        return f"SKIP: {file_name} exists."
    except ClientError:
        pass

    # Parallel Ingest
    print(f"Ingesting: {file_name}")
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        s3.upload_fileobj(response.raw, DEST_BUCKET, s3_key)
        return f"SUCCESS: {file_name}"
    else:
        return f"NOT_FOUND: {file_name} (Status {response.status_code})"

# Execute parallel ingest
results = tasks_rdd.map(ingest_data_type).collect()
for res in results:
    print(res)
