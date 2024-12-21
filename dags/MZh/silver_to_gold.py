import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, TimestampType
from datetime import datetime

# Ініціалізація сесії Spark
spark = SparkSession.builder \
    .appName("Join and Calculate Averages") \
    .getOrCreate()

athlete_bio = spark.read.parquet("dags/silver/athlete_bio")
athlete_event_results = spark.read.parquet("dags/silver/athlete_event_results")

athlete_bio = athlete_bio.withColumn("weight", athlete_bio["weight"].cast(DoubleType()))
athlete_bio = athlete_bio.withColumn("height", athlete_bio["height"].cast(DoubleType()))

athlete_bio = athlete_bio.withColumnRenamed("country_noc", "bio_country_noc")
athlete_event_results = athlete_event_results.withColumnRenamed("country_noc", "event_country_noc")

# Виконуємо join за колонкою athlete_id
joined_df = athlete_bio.join(athlete_event_results, athlete_bio.athlete_id == athlete_event_results.athlete_id, "inner")

# Обчислюємо середні значення для кожної комбінації
avg_stats = joined_df.groupBy("sport", "medal", "sex", "bio_country_noc") \
    .agg(F.avg("weight").alias("avg_weight"), F.avg("height").alias("avg_height"))

# Додаємо колонку timestamp
timestamp = datetime.now()
avg_stats = avg_stats.withColumn("timestamp", F.lit(timestamp).cast(TimestampType()))

# Запис даних у форматі Parquet
avg_stats.write.parquet("dags/gold/avg_stats")

avg_stats.show(truncate=False)

# Зупинка сесії Spark
spark.stop()
