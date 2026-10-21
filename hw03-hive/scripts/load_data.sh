#!/bin/bash
# Скрипт автоматической загрузки данных о книгах в таблицу Hive

#  Подготовка тестовых данных
echo "1,Master and Margarita,Mikhail Bulgakov,Fiction" > /tmp/books_2023.csv
echo "2,1984,George Orwell,Dystopia" >> /tmp/books_2023.csv
echo "3,War and Peace,Leo Tolstoy,Novel" > /tmp/books_2022.csv

#  Загрузка файлов в HDFS
hdfs dfs -mkdir -p /tmp/hive_library
hdfs dfs -put -f /tmp/books_2023.csv /tmp/hive_library/
hdfs dfs -put -f /tmp/books_2022.csv /tmp/hive_library/

#  Создание партиционированной таблицы и загрузка данных через Beeline
beeline -u "jdbc:hive2://team-37-en:10000/default" -n team -e "
CREATE TABLE IF NOT EXISTS books (
    id INT,
    title STRING,
    author STRING,
    genre STRING
) PARTITIONED BY (year INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA INPATH '/tmp/hive_library/books_2023.csv' INTO TABLE books PARTITION (year=2023);
LOAD DATA INPATH '/tmp/hive_library/books_2022.csv' INTO TABLE books PARTITION (year=2022);
"
