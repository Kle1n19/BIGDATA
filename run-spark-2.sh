#!/bin/bash

echo "Submitting Spark job to spark-master..."

docker exec -e HOME=/tmp -e SPARK_IVY_HOME=/tmp/.ivy2 spark-master spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 3G --total-executor-cores 6 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 \
  --conf spark.driver.extraJavaOptions="-Duser.home=/tmp" \
  /opt/spark_app/output.py

echo "Spark job submission command executed."