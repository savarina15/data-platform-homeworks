from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Создаем Spark-сессию с поддержкой Hive
spark = SparkSession.builder \
    .appName("HW4_Spark_Transformation") \
    .enableHiveSupport() \
    .getOrCreate()

# Читаем данные из таблицы books, загруженной в HW3
df = spark.read.table("books")

# Применяем трансформацию: фильтруем книги 2023 года и считаем количество по жанрам
transformed_df = df.filter(col("year") == 2023) \
    .groupBy("genre") \
    .count()

# Сохраняем как таблицу в Hive с партиционированием по жанру
transformed_df.write \
    .mode("overwrite") \
    .partitionBy("genre") \
    .saveAsTable("books_stats_2023")
