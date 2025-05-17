from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType

# Create Spark session
spark = SparkSession.builder.appName("WikimediaKafkaMerged").getOrCreate()

# Define schema including all fields from both scripts
schema = StructType([
    StructField("meta", StructType([
        StructField("domain", StringType(), True),
        StructField("dt", StringType(), True)
    ]), True),
    StructField("performer", StructType([
        StructField("user_id", LongType(), True),
        StructField("user_is_bot", BooleanType(), True),
        StructField("user_text", StringType(), True)
    ]), True),
    StructField("page_title", StringType(), True),
    StructField("page_id", LongType(), True)
])

# Read Kafka input stream
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-server:9092") \
    .option("subscribe", "input") \
    .option("startingOffsets", "latest") \
    .load()

# Parse the JSON and select fields
df = df_raw.select(
    F.from_json(F.col("value").cast("string"), schema).alias("data")
).select(
    F.to_timestamp("data.meta.dt", "yyyy-MM-dd'T'HH:mm:ssX").alias("time"),
    F.col("data.meta.domain").alias("domain"),
    F.col("data.performer.user_is_bot").alias("created_by_bot"),
    F.col("data.performer.user_id").alias("user_id"),
    F.col("data.performer.user_text").alias("username"),
    F.col("data.page_title").alias("page_title"),
    F.col("data.page_id").alias("page_id")
)

# Convert to JSON for output
df_out = df.selectExpr(
    "to_json(struct(time, domain, created_by_bot, user_id, username, page_title, page_id)) AS value"
)

# Write to Kafka output stream
query = df_out.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-server:9092") \
    .option("topic", "processed") \
    .option("checkpointLocation", "/tmp/spark-checkpoints/processed") \
    .start()

# Await termination
query.awaitTermination()
