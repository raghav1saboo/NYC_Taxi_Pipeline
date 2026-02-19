import sys
import requests
import boto3
from botocore.exceptions import ClientError
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions

# Initialize
sc = SparkContext()
args = getResolvedOptions(sys.argv, ['BUCKET_NAME', 'START_YEAR', 'END_YEAR'])
BUCKET = args['BUCKET_NAME']
# We process a range now
YEARS = list(range(int(args['START_YEAR']), int(args['END_YEAR']) + 1))
TAXI_TYPES = ['yellow', 'green', 'fhv', 'fhvhv']

def download_to_s3(task):
    taxi_type, year, month = task
    file_name = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    source_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
    s3_key = f"taxi_data/type={taxi_type}/year={year}/month={month:02d}/{file_name}"
    
    s3 = boto3.client('s3')
    
    # 1. Idempotency Check
    try:
        s3.head_object(Bucket=BUCKET, Key=s3_key)
        return f"SKIP: {file_name} exists."
    except ClientError:
        pass # File doesn't exist, proceed

    # 2. Download and Stream to S3
    try:
        with requests.get(source_url, stream=True) as r:
            if r.status_code == 200:
                s3.upload_fileobj(r.raw, BUCKET, s3_key)
                return f"SUCCESS: {file_name} uploaded."
            else:
                return f"MISSING: {file_name} (Status {r.status_code})"
    except Exception as e:
        return f"ERROR: {file_name} - {str(e)}"

# Create the "Industrial" Task List (e.g., 4 types * 4 years * 12 months = 192 tasks)
tasks = [(t, y, m) for t in TAXI_TYPES for y in YEARS for m in range(1, 13)]

# Parallelize with Spark
results = sc.parallelize(tasks, len(tasks)).map(download_to_s3).collect()

for res in results:
    print(res)
