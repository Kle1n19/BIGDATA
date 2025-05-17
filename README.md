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
docker run --rm -it --network spark-network --name spark-submit -v ./spark:/opt/app bitnami/spark:3 /bin/bash
cd /opt/app
spark-submit  --conf spark.jars.ivy=/opt/app --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0" --total-executor-cores 1 --master spark://spark:7077 --deploy-mode client input.py
```
4. Аналогічно запускаємо *Spark Streaming* для обробки інформації з топіку *processed* й запису в Cassandra
```bash
docker run --rm -it --network spark-network --name spark-submit-2 -v ./spark:/opt/app bitnami/spark:3 /bin/bash
cd /opt/app
spark-submit  --conf spark.jars.ivy=/opt/app --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0 --total-executor-cores 1 --master spark://spark:7077 --deploy-mode client output.py 
```
5. Тепер запускаємо *Spark* для читання даних з *Cassandra* та їх обробки. Також відповідає за щогодинне оновлення. 
```bash
docker run --rm -it --network spark-network --name spark-submit-2 -v ./spark:/opt/app bitnami/spark:3 /bin/bash
cd /opt/app
spark-submit  --conf spark.jars.ivy=/opt/app --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0 --total-executor-cores 1 --master spark://spark:7077 --deploy-mode client process.py 
```
6. Для API слід було замаунтити папку з вихідними даними щоб можна було її прочитати й повернути результат користувачу.
```bash
cd api
docker build -t my-api .
docker run -d --name my-fastapi-api --network spark-network -v ./spark/output:/output -p 8000:8000 my-api
```
## Результати
## Запис в *Cassandra*:
![](/images/domain_page.png)
![](/images/bots.png)
## Результати форматування
Для ознайомлення слід відкрити `./spark/output`
## API
### Top 20 users
![](/images/top20.png)
### Statistics about the number of pages created by bots for each of the domains for the last 6 hours
![](/images/bot_stat.png)
### Aggregated statistics containing the number of created pages for each Wikipedia domain for each hour in the last 6 hours
![](/images/domain_stat.png)
