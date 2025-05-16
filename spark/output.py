from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType, TimestampType

event_schema = StructType([
    StructField("time", TimestampType()),
    StructField("domain", StringType()),
    StructField("created_by_bot", BooleanType()),
    StructField("user_id", LongType()),
    StructField("username", StringType()),
    StructField("page_title", StringType())
])

def save_to_cassandra(batch_df, batch_id):
    domain_stats_df = batch_df.select("time", "domain", "created_by_bot").dropDuplicates()
    domain_stats_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="domain_page_stats", keyspace="wiki") \
        .save()

    user_stat_df = batch_df.select("time", "user_id", "username", "page_title").dropDuplicates()
    user_stat_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="user_stat", keyspace="wiki") \
        .save()

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("KafkaToCassandraWriter") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.cassandra.output.consistency.level", "LOCAL_QUORUM") \
        .getOrCreate()

    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka-server:9092") \
        .option("subscribe", "processed") \
        .option("startingOffsets", "latest") \
        .load()

    parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING) AS json_value") \
        .select(from_json("json_value", event_schema).alias("event")) \
        .select("event.*")

    streaming_query = parsed_stream.writeStream \
        .foreachBatch(save_to_cassandra) \
        .option("checkpointLocation", "/tmp/spark-checkpoints/write-to-cassandra") \
        .start()

    streaming_query.awaitTermination()
