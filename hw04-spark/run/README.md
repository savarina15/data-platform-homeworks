# HW4: запуск Spark в YARN

run_on_yarn.sh запускает hw04-spark/scripts/spark_task.py через spark-submit --master yarn (cluster mode). Задача читает таблицу books из Hive и пишет books_stats_2023 с партициями по genre.

Запускать на edge из корня репы:

```bash
bash hw04-spark/run/run_on_yarn.sh
```

Скрипт проверяет, что в YARN статус SUCCEEDED и в HDFS появились партиции. Если все ок, пишет RESULT: PASS. Лог в /tmp/hw4-darkhan-*.log.

Посмотреть задачу в UI: ssh -N -L 8088:team-37-nn:8088 team@178.236.27.186, потом localhost:8088.