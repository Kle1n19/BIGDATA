from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType, TimestampType
import time

# Define schema with all fields from both scripts
event_schema = StructType([
    StructField("time", TimestampType()),
    StructField("domain", StringType()),
    StructField("created_by_bot", BooleanType()),
    StructField("user_id", LongType()),
    StructField("username", StringType()),
    StructField("page_title", StringType()),
    StructField("page_id", LongType())
])

def save_to_cassandra(batch_df, batch_id):
    try:
        # Save to domain_page_stats
        batch_df.select("time", "domain", "created_by_bot") \
            .dropDuplicates() \
            .write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="domain_page_stats", keyspace="wiki") \
            .save()
        
        # Save to user_stat
        batch_df.select("time", "user_id", "username", "page_title") \
            .dropDuplicates() \
            .write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="user_stat", keyspace="wiki") \
            .save()

        # Save to distinct_domains
        batch_df.select("domain") \
            .dropDuplicates() \
            .write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="distinct_domains", keyspace="wiki") \
            .save()

        # Save to pages_by_user
        batch_df.select("user_id", "page_title", "page_id") \
            .dropDuplicates() \
            .write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="pages_by_user", keyspace="wiki") \
            .save()

        # Save to domain_page_counts
        batch_df.select("domain", "page_id") \
            .dropDuplicates() \
            .write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="domain_page_counts", keyspace="wiki") \
            .save()

        # Save to page_details_by_page_id
        batch_df.select("page_id", "page_title", "domain", "user_id", "username", "created_by_bot", "time") \
            .dropDuplicates() \
            .write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="page_details_by_page_id", keyspace="wiki") \
            .save()

        # Save to page_creations_by_time
        batch_df.select(F.to_date("time").alias("day"), "user_id", "username") \
            .dropDuplicates() \
            .write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="page_creations_by_time", keyspace="wiki") \
            .save()

    except Exception as e:
        print(f"Error in save_to_cassandra: {e}")

if __name__ == "__main__":
    max_retries = 5
    retry_count = 0

    while retry_count < max_retries:
        try:
            spark = SparkSession.builder \
                .appName("KafkaToCassandraWriter") \
                .config("spark.cassandra.connection.host", "cassandra") \
                .config("spark.cassandra.connection.port", "9042") \
                .config("spark.cassandra.output.consistency.level", "LOCAL_QUORUM") \
                .config("spark.cassandra.connection.timeoutMS", "10000") \
                .config("spark.network.timeout", "600s") \
                .config("spark.executor.heartbeatInterval", "120s") \
                .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints/write-to-cassandra") \
                .getOrCreate()

            spark.sparkContext.setLogLevel("WARN")

            kafka_stream = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka-server:9092") \
                .option("subscribe", "processed") \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .option("kafka.request.timeout.ms", "6000") \
                .option("kafka.session.timeout.ms", "6000") \
                .load()

            parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING) as json_value") \
                .select(from_json(col("json_value"), event_schema).alias("event")) \
                .select("event.*")

            streaming_query = parsed_stream.writeStream \
                .foreachBatch(save_to_cassandra) \
                .option("checkpointLocation", "/tmp/spark-checkpoints/write-to-cassandra") \
                .trigger(processingTime="1 second") \
                .start()

            streaming_query.awaitTermination()
            break

        except Exception as e:
            retry_count += 1
            print(f"Streaming failed: {e}, retrying {retry_count}/{max_retries}")
            if retry_count < max_retries:
                time.sleep(retry_count * 10)  # Exponential backoff
            else:
                raise
