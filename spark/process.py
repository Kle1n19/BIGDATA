import time
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, date_trunc, count, collect_list, desc, lit
import json
import os

def get_time_bounds():
    now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    end_excl_last = now - timedelta(hours=1)
    start = end_excl_last - timedelta(hours=8)
    return start, end_excl_last

def format_hour(dt):
    return dt.strftime("%H:%M")

def run_job():
    # Init Spark
    spark = SparkSession.builder \
        .appName("WikiStatsQueries") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    start_time, end_time = get_time_bounds()

    domain_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="domain_page_stats", keyspace="wiki") \
        .load() \
        .filter((col("time") >= lit(start_time)) & (col("time") < lit(end_time)))

    user_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="user_stat", keyspace="wiki") \
        .load() \
        .filter((col("time") >= lit(start_time)) & (col("time") < lit(end_time)))

    domain_with_hour = domain_df.withColumn("hour", date_trunc("hour", col("time")))

    hourly_stats_df = domain_with_hour.groupBy("hour", "domain").agg(count("*").alias("pages_created")).orderBy("hour")

    hourly_result = []
    for hour_row in hourly_stats_df.select("hour").distinct().orderBy("hour").collect():
        hour_start = hour_row["hour"]
        hour_end = hour_start + timedelta(hours=1)
        hour_slice = hourly_stats_df.filter(col("hour") == hour_start)

        stats = [
            {row["domain"]: row["pages_created"]}
            for row in hour_slice.select("domain", "pages_created").collect()
        ]

        hourly_result.append({
            "time_start": format_hour(hour_start),
            "time_end": format_hour(hour_end),
            "statistics": stats
        })

    bot_stats_df = domain_df.filter(col("created_by_bot") == True)
    bot_stats_agg = bot_stats_df.groupBy("domain").agg(count("*").alias("created_by_bots"))
    bot_result = {
        "time_start": format_hour(start_time),
        "time_end": format_hour(end_time),
        "statistics": [
            {"domain": row["domain"], "created_by_bots": row["created_by_bots"]}
            for row in bot_stats_agg.collect()
        ]
    }

    user_grouped = user_df.groupBy("user_id", "username").agg(count("*").alias("pages_created"), collect_list("page_title").alias("page_titles"))

    top_20_users = user_grouped.orderBy(desc("pages_created")).limit(20)

    top_users_result = [
        {
            "user_id": row["user_id"],
            "username": row["username"],
            "time_start": format_hour(start_time),
            "time_end": format_hour(end_time),
            "page_titles": row["page_titles"],
            "pages_created": row["pages_created"]
        }
        for row in top_20_users.collect()
    ]

    os.makedirs("./output", exist_ok=True)

    with open("./output/hourly_domain_page_stats.json", "w") as f:
        json.dump(hourly_result, f, indent=2)

    with open("./output/bot_created_domain_stats.json", "w") as f:
        json.dump(bot_result, f, indent=2)

    with open("./output/top_20_users.json", "w") as f:
        json.dump(top_users_result, f, indent=2)

    spark.stop()

if __name__ == "__main__":
    while True:
        print(f"Running job at {datetime.utcnow().isoformat()} UTC")
        try:
            run_job()
        except Exception as e:
            print(f"Error during job run: {e}")
        print("Sleeping for 1 hour...")
        time.sleep(3600)
