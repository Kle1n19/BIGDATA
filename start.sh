#!/bin/bash


docker compose up -d
sleep 15

docker exec kafka kafka-topics --create --topic input_stream --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1
docker exec kafka kafka-topics --create --topic output_stream --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1

sleep 10



echo "Kafka topics created."


echo "Starting Cassandra node (cassandra-node1)..."
docker run -d \
  --name cassandra-node1 \
  --network spark_network \
  -p 9042:9042 \
  cassandra:4.1


until docker exec cassandra-node1 cqlsh -e "describe cluster" > /dev/null 2>&1; do
  echo "Waiting for Cassandra to be ready..."
  sleep 5
done

docker cp schema.cql cassandra-node1:schema.cql
docker exec -it cassandra-node1 cqlsh -f schema.cql
echo "schema"



