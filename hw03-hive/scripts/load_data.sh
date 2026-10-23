#!/bin/bash
# Скрипт автоматической загрузки данных о книгах в таблицу Hive
set -euo pipefail

raw_path="${RAW_PATH:-/user/team/datasets/books}"
staging_path="${STAGING_PATH:-/user/team/staging/hive_books}"
jdbc_url="${JDBC_URL:-jdbc:hive2://team-37-en:10000/default}"
hive_user="${HIVE_USER:-team}"

data_dir="$(mktemp -d)"
trap 'rm -rf "$data_dir"' EXIT

# Подготовка тестовых данных
echo "1,Master and Margarita,Mikhail Bulgakov,Fiction" > "$data_dir/books_2023.csv"
echo "2,1984,George Orwell,Dystopia" >> "$data_dir/books_2023.csv"
echo "3,War and Peace,Leo Tolstoy,Novel" > "$data_dir/books_2022.csv"

# Загрузка файлов в HDFS: исходники для Spark и копии для Hive
for year in 2022 2023; do
    hdfs dfs -mkdir -p "$raw_path/year=$year" "$staging_path"
    hdfs dfs -put -f "$data_dir/books_$year.csv" "$raw_path/year=$year/books.csv"
    hdfs dfs -put -f "$data_dir/books_$year.csv" "$staging_path/books_$year.csv"
done

# Создание партиционированной таблицы и загрузка данных через Beeline
beeline -u "$jdbc_url" -n "$hive_user" --force=false -e "
CREATE TABLE IF NOT EXISTS default.books (
    id INT,
    title STRING,
    author STRING,
    genre STRING
) PARTITIONED BY (year INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Повторная загрузка заменяет только две учебные партиции.
LOAD DATA INPATH '$staging_path/books_2022.csv'
OVERWRITE INTO TABLE default.books PARTITION (year=2022);

LOAD DATA INPATH '$staging_path/books_2023.csv'
OVERWRITE INTO TABLE default.books PARTITION (year=2023);

SHOW PARTITIONS default.books;
SELECT year, COUNT(*) AS books_count
FROM default.books GROUP BY year ORDER BY year;
SELECT id, title, author, genre, year FROM default.books ORDER BY id;
"
