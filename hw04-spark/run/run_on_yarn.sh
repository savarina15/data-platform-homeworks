#!/usr/bin/env bash
# HW4: Spark-задача целиком внутри YARN (драйвер тоже в контейнере)

spark=/home/team/spark-3.5.1-bin-hadoop3/bin/spark-submit
hdfs=/home/team/hadoop-3.4.0/bin/hdfs
rm_host=team-37-nn
result=/user/hive/warehouse/books_stats_2023
run=hw4-darkhan-$(date +%H%M%S)
log=/tmp/$run.log

$spark --master yarn --deploy-mode "${MODE:-cluster}" --name "$run" \
  --conf spark.hadoop.yarn.resourcemanager.hostname=$rm_host \
  --conf spark.hadoop.hive.metastore.uris=thrift://team-37-en:9083 \
  --num-executors 2 --executor-memory 512m --driver-memory 512m \
  hw04-spark/scripts/spark_task.py &> "$log"

# статус задачи у YARN и партиции результата в HDFS
app_id=$(grep -om1 'application_[0-9]*_[0-9]*' "$log")
final=$(curl -s "http://$rm_host:8088/ws/v1/cluster/apps/$app_id" | grep -o '"finalStatus":"[A-Z]*"' | cut -d'"' -f4)
genres=$($hdfs dfs -ls "$result" 2>/dev/null | grep -c 'genre=')

echo "$run ($app_id): $final, партиций в HDFS: $genres"

if [ "$final" = SUCCEEDED ] && [ "$genres" -gt 0 ]; then
  echo "RESULT: PASS"
else
  echo "RESULT: FAIL, лог: $log"
  exit 1
fi