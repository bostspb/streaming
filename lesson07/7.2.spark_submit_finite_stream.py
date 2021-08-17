from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType
import datetime

# source path
source_path = 'process_csv_as_stream'
output_path = "tmp/file_output"
checkpoint_location = "tmp/checkpoint"

spark = SparkSession.builder.appName("spark-submit-finite-stream-app").getOrCreate()

schema = StructType() \
    .add("product_category_name", StringType()) \
    .add("product_category_name_english", StringType())

# read all csv in stream mode
raw_files = spark \
    .readStream \
    .format("csv") \
    .schema(schema) \
    .options(path=source_path, header=True) \
    .load()

# set time once
load_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")


def file_output(df, freq):
    date = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    return df.writeStream.format("parquet") \
        .trigger(processingTime='%s seconds' % freq) \
        .option("path", output_path + "/p_date=" + str(load_time)) \
        .option("checkpointLocation", checkpoint_location + "/" + date) \
        .start()


timed_files = raw_files.withColumn("p_date", F.lit(load_time))

# stream start
stream = file_output(timed_files, 10)

# will always spark.stop() at the end

# STREAM STOPS BECAUSE THESE ALWAYS IS A SPARK.STOP() IN THE END
