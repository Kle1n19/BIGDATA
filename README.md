# Bigdata Project
## Джерело даних

Джерело — Wikimedia EventStreams API: [https://stream.wikimedia.org/v2/stream/page-create](https://stream.wikimedia.org/v2/stream/page-create). Це API надає оновлення в реальному часі про події створення сторінок у Вікіпедії.

## Компоненти та архітектурні рішення
### 1. **Docker та Docker Compose**

- **Навіщо:** Стабільне середовище розробки(у випадку зі `Spark` не врятувало).
- **Використання:**
  - Контейнери для Spark (майстер та воркер), Kafka, Zookeeper, Cassandra та API.
  - Скрипт `run.sh` ініціалізує мережу та сервіси.

### 2. **Kafka**

- **Навіщо:** Kafka — система для обробки потокових даних у реальному часі.
- **Використання:**
  - Топіки Kafka (`input` і `processed`) використовуються для отримання даних як є та передачі оброблених далі.
  - Сервіс `generate` надсилає дані в `input` тоопік, а Spark обробляє їх і передає у `processed`.

### 3. **Spark**

- **Навіщо:** Apache Spark — потужний фреймворк для розподіленої обробки даних. Підтримує потокову та batch processing.
- **Використання:**
  - `input.py`: читає дані з теми `input`, обробляє їх і пише у `processed`.
  - `output.py`: читає з `processed` і записує в Cassandra.
  - `process.py`: виконує періодичну обробку даних з Cassandra.

### 4. **Cassandra**

- **Навіщо:** Cassandra — NoSQL база даних, оптимізована для високої доступності та масштабування, ідеально підходить для time series processing.
- **Використання:**
  - Дві таблиці (`domain_page_stats` і `user_stat`) зберігають оброблені аналітичні дані.
  - Схема визначена у `schema.cql`. Архітектура схеми була розроблена з урахуванням необіхідної інформації що слід обробляти й отримувати.  

### 5. **Kafka Producer**

- **Навіщо:**  Kafka Producer — для надсилання даних у Kafka.
- **Використання:**
  - `generate/main.py`: отримує дані з Wikimedia API та надсилає в Kafka `input`.

### 6. **API**

- **Навіщо:** Доступ до оброблених даних через HTTP-запити.
- **Використання:**
  - Сервіс на базі FastAPI надає ендпоінти для запиту даних з Cassandra. Для швидкодії вони читають вже готову відповідь, а не чекають на її отримання
  - 
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
docker run -d --name my-fastapi-api -network spark-network -v ./spark/output:/output -p 8000:8000 my-api
```
### Результати
### Запис в *Cassandra*:
![](/images/domain_page.png)
![](/images/bots.png)
### Результати форматування
Для ознайомлення слід відкрити `./spark/output`
### API
#### Top 20 users
![](/images/top20.png)
#### Statistics about the number of pages created by bots for each of the domains for the last 6 hours
![](/images/bot_stat.png)
#### Aggregated statistics containing the number of created pages for each Wikipedia domain for each hour in the last 6 hours
![](/images/domain_stat.png)
