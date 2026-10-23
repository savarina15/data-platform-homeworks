import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

parser = argparse.ArgumentParser(description="Подсчёт книг по жанрам из CSV в HDFS")
parser.add_argument("--input", default="hdfs://team-37-nn:9000/user/team/datasets/books")
parser.add_argument("--output", default="hdfs://team-37-nn:9000/user/hive/warehouse/books_stats_2023")
parser.add_argument("--year", type=int, default=2023)
args = parser.parse_args()

# Создаем Spark-сессию; мастер задаётся при запуске через spark-submit
spark = SparkSession.builder \
    .appName("HW4_Spark_Transformation") \
    .getOrCreate()

try:
    # Читаем исходные CSV из HDFS, сохранённые в HW3; год — из каталогов year=...
    df = spark.read.option("header", "false") \
        .option("mode", "FAILFAST") \
        .option("basePath", args.input) \
        .schema("id STRING, title STRING, author STRING, genre STRING") \
        .csv(args.input)

    # Приводим типы и убираем пробелы по краям жанра
    df = df.withColumn("id", col("id").cast("int")) \
        .withColumn("year", col("year").cast("int")) \
        .withColumn("genre", trim(col("genre")))

    # Фильтруем книги выбранного года и считаем количество по жанрам
    transformed_df = df.filter(
        (col("year") == args.year) & (col("id") > 0) & (col("genre") != "")
    ).groupBy("genre").count()

    # Сохраняем Parquet с партиционированием по жанру; таблицу Hive регистрирует QA
    transformed_df.write \
        .mode("overwrite") \
        .partitionBy("genre") \
        .parquet(args.output)
finally:
    spark.stop()
