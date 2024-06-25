from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as spark_sum, max as spark_max, trim, date_trunc
from pyspark.sql.window import Window
import os
import shutil

# Create Spark session
# PC has 4 cores so can use 2 cores for this job
# Unnecessary but good practice. 
spark = SparkSession.builder \
    .appName('AppleVsMicrosoft') \
    .config("spark.cores.max", "2") \
    .getOrCreate()

apple_file = './AAPL_minute_data.csv'
microsoft_file = './MSFT_minute_data.csv'

# inferSchema=True will automatically infer the data types of each column
# header=True will use the first row as the column names
df_apple = spark.read.csv(apple_file, header=True, inferSchema=True)
df_microsoft = spark.read.csv(microsoft_file, header=True, inferSchema=True)

# Combine data into a single DataFrame so we can perform aggregations on both stocks
df = df_apple.union(df_microsoft)

# Weird whitespace in the stock_symbol column?
df = df.withColumn('stock_symbol', trim(col('stock_symbol')))

# Convert and extract timestamp, date, and hour
df = df.withColumn('timestamp', col('timestamp').cast('timestamp'))
df = df.withColumn('date', col('timestamp').cast('date'))
df = df.withColumn('hour', date_trunc('hour', col('timestamp')))

#Spark has several in-built functions for aggregation
# spark_sum, spark_max, spark_min, spark_avg, spark_count, spark_approx_count_distinct, spark_collect_list, spark_collect_set, spark_first, spark_last, spark_kurtosis, spark_skewness, spark_stddev, spark_variance

# Daily averages
daily_avg = df.groupBy('stock_symbol', 'date').agg(
    avg('open').alias('avg_open'),
    avg('high').alias('avg_high'),
    avg('low').alias('avg_low'),
    avg('close').alias('avg_close'),
    spark_sum('volume').alias('total_volume')
).orderBy('stock_symbol', 'date')

# Hourly maximum prices
hourly_max = df.groupBy('stock_symbol', 'date', 'hour').agg(
    spark_max('high').alias('max_high')
).orderBy('stock_symbol', 'date', 'hour')

# Total volume per day
daily_volume = df.groupBy('stock_symbol', 'date').agg(
    spark_sum('volume').alias('total_volume')
).orderBy('stock_symbol', 'date')

# Moving average of closing prices
window_spec = Window.partitionBy('stock_symbol').orderBy('timestamp').rowsBetween(Window.unboundedPreceding, Window.currentRow)
df = df.withColumn('moving_avg_close', avg('close').over(window_spec))

# Save aggregated data to local storage for later use
daily_avg_path = './daily_avg'
hourly_max_path = './hourly_max'
daily_volume_path = './daily_volume'
moving_avg_close_path = './moving_avg_close'

daily_avg.write.csv(daily_avg_path, header=True, mode='overwrite')
hourly_max.write.csv(hourly_max_path, header=True, mode='overwrite')
daily_volume.write.csv(daily_volume_path, header=True, mode='overwrite')
df.select('timestamp', 'stock_symbol', 'close', 'moving_avg_close').write.csv(moving_avg_close_path, header=True, mode='overwrite')

# Stop Spark session
spark.stop()

# I think you can't output a single file with Spark, so move and rename the files
# Spark outputs the .csv, .csv.crc, and _SUCCESS files

def move_and_rename_files(output_dir, new_filename):
    for file_name in os.listdir(output_dir):
        if file_name.endswith('.csv'):
            src_path = os.path.join(output_dir, file_name)
            dst_path = os.path.join('./', new_filename)
            shutil.move(src_path, dst_path)
            break

move_and_rename_files(daily_avg_path, 'daily_avg.csv')
move_and_rename_files(hourly_max_path, 'hourly_max.csv')
move_and_rename_files(daily_volume_path, 'daily_volume.csv')
move_and_rename_files(moving_avg_close_path, 'moving_avg_close.csv')

# Remove leftover directories

shutil.rmtree(daily_avg_path)
shutil.rmtree(hourly_max_path)
shutil.rmtree(daily_volume_path)
shutil.rmtree(moving_avg_close_path)
