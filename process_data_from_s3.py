import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as spark_sum, max as spark_max, trim, date_trunc
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
import boto3
from dotenv import load_dotenv

import time

# Set up logging / Pinpoint where errors were happening
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# VERY SPECIFIC jar files required for the methods required.
hadoop_aws_jar = "jars/hadoop-aws-3.2.2.jar"
aws_sdk_bundle_jar = "jars/aws-java-sdk-bundle-1.11.901.jar"

load_dotenv()
amazon_key = os.getenv('AMAZON_ACCESS_KEY')
secret_key = os.getenv('AMAZON_SECRET_KEY')

executor_memory = "4g" 
driver_memory = "4g" 
executor_cores = "3"
cores_max = "3"  

# Multi-core setup execution time: 53.03 seconds

# executor_memory = "4g" 
# driver_memory = "4g" 
# executor_cores = "2"
# cores_max = "2"   

# Multi-core setup execution time: 62.55 seconds

# executor_memory = "1g" 
# driver_memory = "1g" 
# executor_cores = "1"
# cores_max = "1"   

# Multi-core setup execution time: 79.08 seconds

try:
    logger.info("Initializing Spark session")
    spark = SparkSession.builder \
        .appName('AppleVsMicrosoft') \
        .config("spark.master", "local[*]") \
        .config("spark.executor.memory", executor_memory) \
        .config("spark.driver.memory", driver_memory) \
        .config("spark.executor.cores", executor_cores) \
        .config("spark.cores.max", cores_max) \
        .config("spark.jars", f"{hadoop_aws_jar},{aws_sdk_bundle_jar}") \
        .config("spark.hadoop.fs.s3a.access.key", amazon_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .getOrCreate()
    logger.info("Spark session initialized successfully")
except Exception as e:
    logger.error("Failed to initialize Spark session", exc_info=True)
    raise

start_time = time.time()

bucket_name = 'simple-stock-analysis'
s3_path_apple = f"s3a://{bucket_name}/AAPL_minute_data.csv"
s3_path_microsoft = f"s3a://{bucket_name}/MSFT_minute_data.csv"

try:
    logger.info("Reading Apple data from S3")
    df_apple = spark.read.csv(s3_path_apple, header=True, inferSchema=True)
    logger.info("Apple data read successfully")
except Exception as e:
    logger.error("Failed to read Apple data from S3", exc_info=True)
    raise

try:
    logger.info("Reading Microsoft data from S3")
    df_microsoft = spark.read.csv(s3_path_microsoft, header=True, inferSchema=True)
    logger.info("Microsoft data read successfully")
except Exception as e:
    logger.error("Failed to read Microsoft data from S3", exc_info=True)
    raise

try:
    logger.info("Combining Apple and Microsoft data")
    df = df_apple.union(df_microsoft)
    logger.info("Data combined successfully")
except Exception as e:
    logger.error("Failed to combine data", exc_info=True)
    raise

try:
    logger.info("Cleaning and processing data")
    df = df.withColumn('stock_symbol', trim(col('stock_symbol')))
    df = df.withColumn('timestamp', col('timestamp').cast('timestamp'))
    df = df.withColumn('date', col('timestamp').cast('date'))
    df = df.withColumn('hour', date_trunc('hour', col('timestamp')))
    logger.info("Data cleaned and processed successfully")
except Exception as e:
    logger.error("Failed to clean and process data", exc_info=True)
    raise

try:
    logger.info("Calculating daily averages")
    daily_avg = df.groupBy('stock_symbol', 'date').agg(
        avg('open').alias('avg_open'),
        avg('high').alias('avg_high'),
        avg('low').alias('avg_low'),
        avg('close').alias('avg_close'),
        spark_sum('volume').alias('total_volume')
    ).orderBy('stock_symbol', 'date')
    logger.info("Daily averages calculated successfully")
except Exception as e:
    logger.error("Failed to calculate daily averages", exc_info=True)
    raise

try:
    logger.info("Calculating hourly maximum prices")
    hourly_max = df.groupBy('stock_symbol', 'date', 'hour').agg(
        spark_max('high').alias('max_high')
    ).orderBy('stock_symbol', 'date', 'hour')
    logger.info("Hourly maximum prices calculated successfully")
except Exception as e:
    logger.error("Failed to calculate hourly maximum prices", exc_info=True)
    raise

try:
    logger.info("Calculating total volume per day")
    daily_volume = df.groupBy('stock_symbol', 'date').agg(
        spark_sum('volume').alias('total_volume')
    ).orderBy('stock_symbol', 'date')
    logger.info("Total volume per day calculated successfully")
except Exception as e:
    logger.error("Failed to calculate total volume per day", exc_info=True)
    raise

try:
    logger.info("Calculating moving average of closing prices")
    window_spec = Window.partitionBy('stock_symbol').orderBy('timestamp').rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df = df.withColumn('moving_avg_close', avg('close').over(window_spec))
    logger.info("Moving average of closing prices calculated successfully")
except Exception as e:
    logger.error("Failed to calculate moving average of closing prices", exc_info=True)
    raise

def save_and_move_to_s3(df, bucket_name, file_name):
    s3_temp_path = f"s3a://{bucket_name}/temp/{file_name}"
    s3 = boto3.client('s3')

    try:
        logger.info(f"Saving {file_name} to S3")
        df.write.csv(s3_temp_path, header=True, mode='overwrite')

        # response is a dictionary with the contents of the bucket so we can iterate over it
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=f"temp/{file_name}")
        for obj in response.get('Contents', []):
            if obj['Key'].endswith('.csv'):
                
                # Copy the file to the root directory with the proper name
                copy_source = {'Bucket': bucket_name, 'Key': obj['Key']}
                s3.copy(copy_source, bucket_name, file_name)
                logger.info(f"Moved and renamed {file_name} to root directory")
                
        # Delete the temp directory (and the success files inside)
        for obj in response.get('Contents', []):
            s3.delete_object(Bucket=bucket_name, Key=obj['Key'])

        logger.info(f"{file_name} saved to S3 successfully")
    except Exception as e:
        logger.error(f"Failed to save {file_name} to S3", exc_info=True)
        raise

save_and_move_to_s3(daily_avg, bucket_name, 'daily_avg.csv')
save_and_move_to_s3(hourly_max, bucket_name, 'hourly_max.csv')
save_and_move_to_s3(daily_volume, bucket_name, 'daily_volume.csv')
save_and_move_to_s3(df.select('timestamp', 'stock_symbol', 'close', 'moving_avg_close'), bucket_name, 'moving_avg_close.csv')

# Stop Spark session
try:
    logger.info("Stopping Spark session")
    spark.stop()
    logger.info("Spark session stopped successfully")
except Exception as e:
    logger.error("Failed to stop Spark session", exc_info=True)
    raise

end_time = time.time()

## How long did this script take to run?

print("Multi-core setup execution time: {:.2f} seconds".format(end_time - start_time))


