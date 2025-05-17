from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType, TimestampType
from pyspark.sql import functions as F
import time


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
        # Save to distinct_domains
        distinct_domains_df = batch_df.select("domain").dropDuplicates()
        distinct_domains_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="distinct_domains", keyspace="wiki") \
            .save()

        # Save to pages_by_user
        pages_by_user_df = batch_df.select("user_id", "page_title", "page_id").dropDuplicates()
        pages_by_user_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="pages_by_user", keyspace="wiki") \
            .save()

        domain_page_df = batch_df.select("domain", "page_id").dropDuplicates()

        domain_page_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="domain_page_counts", keyspace="wiki") \
            .save()

        # Save to page_details_by_page_id
        page_details_df = batch_df.select(
            "page_id", "page_title", "domain", "user_id", "username", "created_by_bot", "time"
        ).dropDuplicates()
        page_details_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="page_details_by_page_id", keyspace="wiki") \
            .save()

        # Update page_creations_by_time (using counters)
        page_creations_by_time_df = batch_df.select(
            F.to_date("time").alias("day"), "user_id", "username"
        ).dropDuplicates()
        page_creations_by_time_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="page_creations_by_time", keyspace="wiki") \
            .save()
        
    except Exception as e:
        print(e)


if __name__ == "__main__":
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            spark = SparkSession.builder \
                .appName("KafkaToCassandraWriter") \
                .config("spark.cassandra.connection.host", "cassandra-node1") \
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
                .option("kafka.bootstrap.servers", "kafka:9092") \
                .option("subscribe", "output_stream") \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .option("kafka.request.timeout.ms", "6000") \
                .option("kafka.session.timeout.ms", "6000") \
                .load()

            parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING) as json_value")

            parsed_stream = parsed_stream.select(from_json(col("json_value"), event_schema).alias("event")).select("event.*")
            
            streaming_query = parsed_stream.writeStream \
                .foreachBatch(save_to_cassandra) \
                .option("checkpointLocation", "/tmp/spark-checkpoints/write-to-cassandra") \
                .trigger(processingTime="10 seconds") \
                .start()
            
            streaming_query.awaitTermination()
            break 
            
        except Exception as e:
            retry_count += 1
            if retry_count < max_retries:
                wait_time = retry_count * 10  # Exponential backoff
                time.sleep(wait_time)
            else:
                raise