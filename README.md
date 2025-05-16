# Bigdata Project
## Running 
1. Надайте дозвіл та запустіть 
```bash
./run.sh
```  
2. Запускаємо контейнер що читатиме дані з ендпойнта й передаватиме дані в **input** за допомогою
```bash
docker build -t generate ./generate
docker run --rm --network spark-network generate 
```
3. Запускаємо *Spark Streaming* для переносу даних з *input* в  *processed*

```bash
docker run --rm -it --network spark-network --name spark-submit-1 -v ./spark:/opt/app bitnami/spark:3 /bin/bash
cd /opt/app
spark-submit  --conf spark.jars.ivy=/opt/app --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0" --total-executor-cores 1 --master spark://spark:7077 --deploy-mode client input.py
```
4. Аналогічно запускаємо *Spark Streaming* для обробки інформації з топіку *processed* й запису в Cassandra
```bash
docker run --rm -it --network spark-network --name spark-submit-2 -v ./spark:/opt/app bitnami/spark:3 /bin/bash
cd /opt/app
spark-submit  --conf spark.jars.ivy=/opt/app --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0 --total-executor-cores 1 --master spark://spark:7077 --deploy-mode client output.py 
```


