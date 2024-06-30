import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as spark_sum, max as spark_max, trim, date_trunc
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
import boto3
from dotenv import load_dotenv
import time
from functools import wraps
from contextlib import contextmanager

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

def handle_exceptions(func):
    # Preserve the original function's name and docstring
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"An error occurred in {func.__name__}: {e}", exc_info=True)
            raise
    return wrapper

@contextmanager
def spark_session_context(app_name, config):
    try:
        logger.info("Initializing Spark session")
        spark = SparkSession.builder.appName(app_name)
        for key, value in config.items():
            spark = spark.config(key, value)
        spark = spark.getOrCreate()
        yield spark
    except Exception as e:
        logger.error("Failed to initialize Spark session", exc_info=True)
        raise
    finally:
        try:
            logger.info("Stopping Spark session")
            spark.stop()
            logger.info("Spark session stopped successfully")
        except Exception as e:
            logger.error("Failed to stop Spark session", exc_info=True)
            raise

spark_config = {
    "spark.master": "local[*]",
    "spark.executor.memory": executor_memory,
    "spark.driver.memory": driver_memory,
    "spark.executor.cores": executor_cores,
    "spark.cores.max": cores_max,
    "spark.jars": f"{hadoop_aws_jar},{aws_sdk_bundle_jar}",
    "spark.hadoop.fs.s3a.access.key": amazon_key,
    "spark.hadoop.fs.s3a.secret.key": secret_key,
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.endpoint": "s3.amazonaws.com",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
}

bucket_name = 'simple-stock-analysis'
s3_path_apple = f"s3a://{bucket_name}/AAPL_minute_data.csv"
s3_path_microsoft = f"s3a://{bucket_name}/MSFT_minute_data.csv"

@handle_exceptions
def read_data_from_s3(spark, path):
    #inferSchema = True => Automatically infer data types (false = string)
    return spark.read.csv(path, header=True, inferSchema=True)

@handle_exceptions
def process_data(df):
    # withColumn = add or replace a column with the specified value or result of an expression
    # col(xxx) = get the column xxx. If it doesn't exist, it will throw an error.
    
    # Had a strange issue where the stock_symbol had space at the end
    df = df.withColumn('stock_symbol', trim(col('stock_symbol')))
    # Cast the timestamp to a timestamp (visually no difference but it's a different data type)
    df = df.withColumn('timestamp', col('timestamp').cast('timestamp'))
    # Date doesnt exist so creates a new column taking the date from the timestamp
    df = df.withColumn('date', col('timestamp').cast('date'))
    # Truncate so we can group by hour later 
    # (edit: left the date so date shows when the user hovers over the graph)
    df = df.withColumn('hour', date_trunc('hour', col('timestamp')))
    return df

@handle_exceptions
def calculate_daily_averages(df):
    return df.groupBy('stock_symbol', 'date').agg(
        avg('open').alias('avg_open'),
        avg('high').alias('avg_high'),
        avg('low').alias('avg_low'),
        avg('close').alias('avg_close'),
        # Don't use but want to later create a pie chart (% of total?)
        spark_sum('volume').alias('total_volume')
    ).orderBy('stock_symbol', 'date')

@handle_exceptions
def calculate_hourly_max(df):
    # Specificity: i.e. it has to be the same stock_symbol -> date, and then -> hour to be grouped together.
    # The columns are stock_symbol, date, and hour plus the max_high
    return df.groupBy('stock_symbol', 'date', 'hour').agg(
        spark_max('high').alias('max_high')
    # Order of the rows: Apple and Microsoft, then by date, then by hour.
    ).orderBy('stock_symbol', 'date', 'hour')

@handle_exceptions
def calculate_daily_volume(df):
    return df.groupBy('stock_symbol', 'date').agg(
        spark_sum('volume').alias('total_volume')
    ).orderBy('stock_symbol', 'date')

@handle_exceptions
def calculate_moving_avg_close(df):
    #PartitionBy = group the data by the stock_symbol
    # Window = a range of rows in relation to the current row (specified with Window.currentRow)
    # In this case, it's all the rows from the beginning of the partition (Window.unboundedPreceding) to the current row
    # Why no groupBy? groupBy collapses the data into a single row for each group. We need to be able to operate on each row individually.
    window = Window.partitionBy('stock_symbol').orderBy('timestamp').rowsBetween(Window.unboundedPreceding, Window.currentRow)
    #.over = take the average of the close column over the window (i.e. for each row, take the average up until that point)
    return df.withColumn('moving_avg_close', avg('close').over(window))

def save_and_move_to_s3(df, bucket_name, file_name):
    s3_temp_path = f"s3a://{bucket_name}/temp/{file_name}"
    s3 = boto3.client('s3')

    try:
        logger.info(f"Saving {file_name} to S3")
        df.write.csv(s3_temp_path, header=True, mode='overwrite')

        # response is a dictionary with the contents of the bucket so we can iterate over it
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=f"temp/{file_name}")
        # Contents is an array of dictionaries, each containing the metadata of something in the bucket
        for obj in response.get('Contents', []):
            # Key is the name of each file
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

start_time = time.time()

# with keyword = shows the context manager where to '__enter__' and '__exit__' the code. 

with spark_session_context("AppleVsMicrosoft", spark_config) as spark:
    df_apple = read_data_from_s3(spark, s3_path_apple)
    df_microsoft = read_data_from_s3(spark, s3_path_microsoft)
    df = df_apple.union(df_microsoft)
    df = process_data(df)
    daily_avg = calculate_daily_averages(df)
    hourly_max = calculate_hourly_max(df)
    daily_volume = calculate_daily_volume(df)
    df = calculate_moving_avg_close(df)
    # Using groupBy earlier provided us with exactly what we needed so save as is. 
    save_and_move_to_s3(daily_avg, bucket_name, 'daily_avg.csv')
    save_and_move_to_s3(hourly_max, bucket_name, 'hourly_max.csv')
    save_and_move_to_s3(daily_volume, bucket_name, 'daily_volume.csv')
    # earlier just added a new column 'moving_avg_close' to the df so need to specify which columns to save.
    # See above why can't use groupBy.
    save_and_move_to_s3(df.select('timestamp', 'stock_symbol', 'close', 'moving_avg_close'), bucket_name, 'moving_avg_close.csv')

end_time = time.time()

## How long did this script take to run?

print("Multi-core setup execution time: {:.2f} seconds".format(end_time - start_time))


