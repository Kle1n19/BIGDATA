docker network create spark-network
docker run -d --name zookeeper-server --network spark-network -e ALLOW_ANONYMOUS_LOGIN=yes bitnami/zookeeper:latest
docker run -d --name kafka-server --network spark-network -e ALLOW_PLAINTEXT_LISTENER=yes -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper-server:2181 bitnami/kafka:3.6.2
echo "kafka"
docker run -d --name cassandra --network spark-network -p 9042:9042 cassandra:4.1
until docker run --rm --network spark-network cassandra:4.1 cqlsh cassandra -e "DESCRIBE KEYSPACES"; do
  sleep 5
done
echo "cassandra"
docker-compose up -d
sleep 15
echo "spark"
docker run -it --rm --network spark-network -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper-server:2181 bitnami/kafka:3.6.2 kafka-topics.sh --create  --bootstrap-server kafka-server:9092 --replication-factor 1 --partitions 3 --topic input
docker run -it --rm --network spark-network -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper-server:2181 bitnami/kafka:3.6.2 kafka-topics.sh --create  --bootstrap-server kafka-server:9092 --replication-factor 1 --partitions 3 --topic processed
echo "topics"
docker cp schema.cql cassandra:schema.cql
docker exec -it cassandra cqlsh -f schema.cql
echo "schema"