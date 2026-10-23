"""Read the HW3 raw HDFS dataset and write book counts partitioned by genre."""

import argparse

from pyspark.sql import SparkSession, functions as F


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="hdfs://team-37-nn:9000/user/team/datasets/books")
    parser.add_argument("--output", default="hdfs://team-37-nn:9000/user/hive/warehouse/books_stats_2023")
    parser.add_argument("--year", type=int, default=2023)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("HW4_Spark_Transformation").getOrCreate()
    try:

        books = (
            spark.read.option("header", "false")
            .option("mode", "FAILFAST")
            .option("basePath", args.input)
            .schema("id STRING, title STRING, author STRING, genre STRING")
            .csv(args.input)
        )
        cleaned = (
            books.withColumn("id", F.col("id").cast("int"))
            .withColumn("year", F.col("year").cast("int"))
            .withColumn("genre", F.trim(F.col("genre")))
            .filter(
                (F.col("year") == args.year)
                & (F.col("id") > 0)
                & (F.col("genre") != "")
            )
        )
        counts = cleaned.groupBy("genre").count()

        counts.write.mode("overwrite").partitionBy("genre").parquet(args.output)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
