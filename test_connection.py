import boto3
from botocore.config import Config
from io import BytesIO

# Having trouble so script to test connection
def test_s3_connection(bucket_name, file_key):
    config = Config(connect_timeout=60, read_timeout=60)
    s3 = boto3.client('s3', config=config)
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        data = obj['Body'].read()
        print("Successfully read data from S3")
    except Exception as e:
        print(f"Error: {e}")

bucket_name = 'simple-stock-analysis'
file_key = 'AAPL_minute_data.csv'
test_s3_connection(bucket_name, file_key)
