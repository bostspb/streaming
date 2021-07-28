from pyspark.sql import SparkSession


# Source (from files, systems, sockets) --> Sink (to files, systems, sockets)
# Source (from files, systems, sockets) --> (Spark processing) --> Sink (to files, systems, sockets)


# Spark session
spark = SparkSession \
    .builder \
    .appName("FileSourceApp") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-stream-chkpnt/") \
    .getOrCreate()


# Rate based DataFrame
file_df = spark \
    .readStream \
    .format("csv") \
    .schema("key INT, value STRING") \
    .load("/tmp/spark-stream/*")


file_stream = file_df \
    .writeStream \
    .format("console") \
    .start()

file_stream.awaitTermination()
