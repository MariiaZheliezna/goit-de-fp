import mysql.connector
import os
from mysql.connector import errorcode
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, avg, from_json, round
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# 1.  Зчитати дані фізичних показників атлетів за допомогою Spark з MySQL таблиці olympic_dataset.athlete_bio 

jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"

# Створення Spark сесії для зчитування даних з MySQL таблиці
spark = SparkSession.builder \
    .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
    .appName("JDBCToKafka") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1") \
    .master("local[*]") \
    .getOrCreate()

# отримуємо дані фізичних показників атлетів з olympic_dataset.athlete_bio
athlete_bio_df = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver='com.mysql.cj.jdbc.Driver',
    dbtable="athlete_bio",
    user=jdbc_user,
    password=jdbc_password) \
    .load()

print("Дані фізичних показників атлетів з olympic_dataset.athlete_bio")
athlete_bio_df.show()

# 2. Відфільтровуємо дані, де показники зросту та ваги є порожніми або не є числами.

athlete_bio_df = athlete_bio_df.withColumn("height", col("height").cast("double"))
athlete_bio_df = athlete_bio_df.withColumn("weight", col("weight").cast("double"))
athlete_bio_df = athlete_bio_df.filter(col("height").isNotNull() & col("weight").isNotNull())

print("Відфільтровано дані, де показники зросту та ваги є порожніми або не є числами")
athlete_bio_df.printSchema()
athlete_bio_df.show()

# 3. Зчитати дані з mysql таблиці athlete_event_results і записати в кафка топік athlete_event_results. 
# Зчитати дані з результатами змагань з Kafka-топіку athlete_event_results. 
# Дані з json-формату необхідно перевести в dataframe-формат, де кожне поле json є окремою колонкою.

# Готуємo топіки і таблиці
kafka_config = {
    "bootstrap.servers": '77.81.230.104:9092',
    "security.protocol": 'SASL_PLAINTEXT',
    "sasl.mechanism": 'PLAIN',
    "sasl.username": 'admin',
    "sasl.password": 'VawEzo1ikLtrA8Ug8THa',
    "queue.buffering.max.messages": 1000000,  # Максимальна кількість повідомлень у черзі
}

admin_client = AdminClient(kafka_config)
topic_list = admin_client.list_topics().topics

if "athlete_event_results_MZh" not in topic_list:
    new_topic = NewTopic("athlete_event_results_MZh", num_partitions=1, replication_factor=1)
    admin_client.create_topics([new_topic])

if "all_data_out_MZh" not in topic_list:
    new_topic = NewTopic("all_data_out_MZh", num_partitions=1, replication_factor=1)
    admin_client.create_topics([new_topic])

# Створюємо таблицю для запису всіх вихідних даних
try:
    cnx = mysql.connector.connect(
        user=jdbc_user,
        password=jdbc_password,
        host='217.61.57.46',
        database='neo_data'
    )
    cursor = cnx.cursor()

    table_description = (
        "CREATE TABLE IF NOT EXISTS athlete_enriched_agg_MZh ("
        "  sport VARCHAR(255),"
        "  medal VARCHAR(255),"
        "  sex VARCHAR(255),"
        "  country_noc VARCHAR(255),"
        "  avg_height DOUBLE,"
        "  avg_weight DOUBLE,"
        "  timestamp TIMESTAMP"
        ")"
    )

    cursor.execute(table_description)
    cnx.commit()
    print("Створено MySQL таблицю: athlete_enriched_agg_MZh")

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist.")
    elif err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Access denied.")
    else:
        print(err)
finally:
    cursor.close()
    cnx.close()

athlete_event_results_df = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver='com.mysql.cj.jdbc.Driver',
    dbtable="athlete_event_results",
    user=jdbc_user,
    password=jdbc_password) \
    .load()

athlete_event_results_df.show()

# Додаємо поточний час та переводимо в JSON формат
athlete_event_results_df = athlete_event_results_df.withColumn("timestamp", current_timestamp())
athlete_event_results_df_json = athlete_event_results_df.selectExpr("CAST(null AS STRING) AS key", "to_json(struct(*)) AS value")

athlete_event_results_df_json.show()

def delivery_report(err, msg): 
    if err is not None: 
        print(f"Delivery failed for message {msg.key()}: {err}")

# Відправка даних до Kafka 
producer = Producer(kafka_config)
for row in athlete_event_results_df_json.collect():
    producer.produce('athlete_event_results_MZh', value=row.value.encode('utf-8'), callback=delivery_report)
producer.flush()


# Зчитування з kafka топіку
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
    .option("subscribe", "athlete_event_results_MZh") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "50000") \
    .load()

# Визначення схеми для даних
schema = StructType([
    StructField("edition", StringType(), True),
    StructField("edition_id", IntegerType(), True),
    StructField("country_noc", StringType(), True),
    StructField("sport", StringType(), True),
    StructField("event", StringType(), True),
    StructField("result_id", IntegerType(), True),
    StructField("athlete", StringType(), True),
    StructField("athlete_id", IntegerType(), True),
    StructField("pos", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("isTeamSport", StringType(), True)
])

# Розпарсити JSON значення
parsed_df = df.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json(col("json_data"), schema).alias("data")) \
    .select("data.*")

print("parsed_df")


result_df = parsed_df.join(athlete_bio_df, on='athlete_id', how='inner') \
    .drop(athlete_bio_df.country_noc) \
    .withColumnRenamed('df_from_kafka.country_noc', 'country_noc') \
    
    
print("result_df")


aggregated_df = result_df.groupBy("sport", "medal", "sex", "country_noc") \
    .agg(
        round(avg("height"), 1).alias("avg_height"),
        round(avg("weight"), 1).alias("avg_weight")
    )
# Додавання стовпця timestamp з датою та часом розрахунку 
aggregated_df = aggregated_df.withColumn("timestamp", current_timestamp())

print(f"\nОб'єднана таблиця:")

# Виведення потоку даних у консоль
query = aggregated_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Очікування завершення запиту
query.awaitTermination()


def foreach_batch_function(batch_df, batch_id):
    print(f"foreach_batch_function started")
    try:
        batch_df.selectExpr("CAST(null AS STRING) AS key", "to_json(struct(*)) AS value") \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
            .option("topic", "all_data_out_MZh") \
            .option("kafka.security.protocol", "SASL_PLAINTEXT") \
            .option("kafka.sasl.mechanism", "PLAIN") \
            .option("kafka.sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='VawEzo1ikLtrA8Ug8THa';") \
            .save()
        print(f"Batch {batch_id} written to Kafka.")
        
        batch_df.write \
            .format("jdbc") \
            .option("url", f"jdbc:mysql://217.61.57.46:3306/neo_data") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "athlete_enriched_agg_MZh") \
            .option("user", jdbc_user) \
            .option("password", jdbc_password) \
            .mode("append") \
            .save()
        print(f"Batch {batch_id} written to MySQL.")
    except Exception as e:
        print(f"Error writing batch {batch_id}: {e}")

query = aggregated_df \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(foreach_batch_function) \
    .start()

query.awaitTermination()