docker-compose down

docker stop kafka-server zookeeper-server

docker rm kafka-server zookeeper-server

docker stop cassandra

docker rm cassandra

docker network rm spark-network
