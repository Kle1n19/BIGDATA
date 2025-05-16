# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col
# from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType, TimestampType

# event_schema = StructType([
#     StructField("time", TimestampType()),
#     StructField("domain", StringType()),
#     StructField("created_by_bot", BooleanType()),
#     StructField("user_id", LongType()),
#     StructField("username", StringType()),
#     StructField("page_title", StringType())
# ])

# def save_to_cassandra(batch_df, batch_id):
#     domain_stats_df = batch_df.select("time", "domain", "created_by_bot").dropDuplicates()
#     domain_stats_df.write \
#         .format("org.apache.spark.sql.cassandra") \
#         .mode("append") \
#         .options(table="domain_page_stats", keyspace="wiki") \
#         .save()

#     user_stat_df = batch_df.select("time", "user_id", "username", "page_title").dropDuplicates()
#     user_stat_df.write \
#         .format("org.apache.spark.sql.cassandra") \
#         .mode("append") \
#         .options(table="user_stat", keyspace="wiki") \
#         .save()

# if __name__ == "__main__":
#     spark = SparkSession.builder \
#         .appName("KafkaToCassandraWriter") \
#         .config("spark.cassandra.connection.host", "cassandra") \
#         .config("spark.cassandra.connection.port", "9042") \
#         .config("spark.cassandra.output.consistency.level", "LOCAL_QUORUM") \
#         .getOrCreate()

#     kafka_stream = spark.readStream \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", "kafka-server:9092") \
#         .option("subscribe", "processed") \
#         .option("startingOffsets", "latest") \
#         .load()

#     parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING) AS json_value") \
#         .select(from_json("json_value", event_schema).alias("event")) \
#         .select("event.*")

#     streaming_query = parsed_stream.writeStream \
#         .foreachBatch(save_to_cassandra) \
#         .option("checkpointLocation", "/tmp/spark-checkpoints/write-to-cassandra") \
#         .start()

#     streaming_query.awaitTermination()
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType, TimestampType
import time

event_schema = StructType([
    StructField("time", TimestampType()),
    StructField("domain", StringType()),
    StructField("created_by_bot", BooleanType()),
    StructField("user_id", LongType()),
    StructField("username", StringType()),
    StructField("page_title", StringType())
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
    
    except Exception as e:
        print(e)
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
                .config("spark.cassandra.connection.timeout_ms", "10000") \
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

            parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING) as json_value")

            parsed_stream = parsed_stream.select(from_json(col("json_value"), event_schema).alias("event")).select("event.*")
            
            streaming_query = parsed_stream.writeStream \
                .foreachBatch(save_to_cassandra) \
                .option("checkpointLocation", "/tmp/spark-checkpoints/write-to-cassandra") \
                .trigger(processingTime="5 seconds") \
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