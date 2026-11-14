import argparse
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

parser = argparse.ArgumentParser(description="Подсчёт книг по жанрам из CSV в HDFS")
parser.add_argument("--input", default="hdfs://team-37-nn:9000/user/team/datasets/books")
parser.add_argument("--output", default="hdfs://team-37-nn:9000/user/team/hw5/books_stats")
parser.add_argument("--year", type=int, default=2023)
parser.add_argument("--table", default="default.hw5_books_stats")
args = parser.parse_args()
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*", args.table):
    parser.error("--table должен иметь вид database.table")
if "'" in args.output or "\\" in args.output:
    parser.error("--output не должен содержать кавычки или бэкслеш")

# Создаем Spark-сессию
spark = SparkSession.builder \
    .appName("HW5_Books_Processing") \
    .enableHiveSupport() \
    .getOrCreate()

try:
    # Читаем CSV из HDFS
    df = spark.read.option("header", "false") \
        .option("mode", "FAILFAST") \
        .option("basePath", args.input) \
        .schema("id STRING, title STRING, author STRING, genre STRING") \
        .csv(args.input)

    # Приводим типы и вырезаем пробелы по краям
    df = df.withColumn("id", col("id").cast("int")) \
        .withColumn("year", col("year").cast("int")) \
        .withColumn("genre", trim(col("genre")))

    # Фильтруем и считаем количество по жанрам
    transformed_df = df.filter(
        (col("year") == args.year) & (col("id") > 0) & (col("genre") != "")
    ).groupBy("genre").count()

    # Сохраняем паркет и регистрируем результат в метасторе
    transformed_df.write \
        .mode("overwrite") \
        .partitionBy("genre") \
        .parquet(args.output)

    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {args.table} (`count` BIGINT)
        PARTITIONED BY (genre STRING)
        STORED AS PARQUET
        LOCATION '{args.output}'
    """)
    # Обновляем партиции
    spark.sql(f"MSCK REPAIR TABLE {args.table} SYNC PARTITIONS")
finally:
    spark.stop()
