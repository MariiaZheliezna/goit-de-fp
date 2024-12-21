import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

# Функція для чистки тексту
def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))

clean_text_udf = udf(clean_text, StringType())

# Ініціалізація сесії Spark
spark = SparkSession.builder \
    .appName("Clean and Deduplicate Data") \
    .getOrCreate()

# Шляхи до таблиць
tables = ["athlete_bio", "athlete_event_results"]

for table in tables:
    # Читання даних із директорії bronze
    df = spark.read.parquet(f"dags/bronze/{table}")

    # Чистка тексту для всіх текстових колонок
    for col_name in df.columns:
        if df.schema[col_name].dataType == StringType():
            df = df.withColumn(col_name, clean_text_udf(col(col_name)))

    # Дедублікація рядків
    df = df.dropDuplicates()

    # Запис даних у форматі Parquet в директорію silver
    df.write.parquet(f"dags/silver/{table}")
    
    df.show(truncate=False)

# Зупинка сесії Spark
spark.stop()
