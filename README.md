# Data Platform Homeworks — Team 37

## Архитектура стенда
| Узел | IP | Роли |
|---|---|---|
| team-37-en | 10.37.0.10 | Edge, Hive Metastore, HiveServer2, Spark Client |
| team-37-nn | 10.37.0.11 | NameNode, SecondaryNameNode, DataNode, ResourceManager, NodeManager |
| team-37-00 | 10.37.0.12 | DataNode, NodeManager |
| team-37-01 | 10.37.0.13 | DataNode, NodeManager |

## Домашние задания
Подробные инструкции и скрипты находятся в соответствующих папках:
* **[HW1: HDFS](./hw01-hdfs/README.md)**
* **[HW2: YARN](./hw02-yarn/README.md)**
* **[HW3: Apache Hive](./hw03-hive/README.md)**
* **[HW4: Apache Spark](./hw04-spark/README.md)**

## Порядок запуска (End-to-End)
1. **Инфраструктура (HW1, HW2):** Развернуть HDFS и YARN.
2. **Hive (HW3):** Развернуть Hive, загрузить данные в партиционированную таблицу.
3. **Spark (HW4):** Запустить PySpark-приложение через YARN для обработки данных и сохранения в Parquet.
4. **Интеграция:** Проверить чтение результатов Spark через внешнюю таблицу Hive.
