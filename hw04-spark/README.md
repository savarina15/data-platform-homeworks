# HW4: Apache Spark под управлением YARN

## Описание
Развернут Apache Spark 3.5.1 для работы под управлением YARN. Реализована задача, которая читает данные из Hive (таблица `books`), выполняет трансформацию (фильтрация по году и агрегация по жанрам), сохраняет результат в Hive с партиционированием.

## Архитектура
- Spark Master: YARN ResourceManager (team-37-nn)
- Hive Metastore: team-37-en (порт 9083)
- HDFS: team-37-nn (порт 9000)

## Установка (Автоматизированная)
Для установки Spark используется Ansible-плейбук:
```bash
cd ~/data-platform-homeworks
ansible-playbook -i shared/inventory.ini hw04-spark/ansible/install-spark.yml
```

## Обработка данных
[`scripts/spark_task.py`](scripts/spark_task.py) читает исходные CSV HW3 из HDFS, приводит типы, фильтрует данные и считает книги по жанрам. Результат записывается в Parquet с партициями по `genre`; регистрацию таблицы выполняет инструмент QA.

## Запуск (Автоматизированный)
Для запуска PySpark-приложения под управлением YARN используется следующий скрипт:
```bash
cd ~/data-platform-homeworks
bash hw04-spark/run/run_on_yarn.sh
```
*(Скрипт выполняет `spark-submit --master yarт` и запускает обработку данных).*

## Проверка (Verification)

### 1. Проверка успешного выполнения в YARN
Убедитесь, что приложение завершилось успешно (статус `FINISHED`):
```bash
yarn application -list -appStates FINISHED
```

### 2. Интеграция с Hive (QA-проверка)
Скрипт QA автоматически создает внешнюю таблицу Hive над Parquet-файлами Spark и проверяет чтение данных. Запуск осуществляется с конфигурационным файлом:
```bash
cd ~/data-platform-homeworks
python3 hw04-spark/qa/hive_integration.py apply --config hw04-spark/qa/spark_hive.team37.json
python3 hw04-spark/qa/hive_integration.py verify --config hw04-spark/qa/spark_hive.team37.json
```
*(Скрипт создает таблицу, регистрирует партиции и проверяет, что данные доступны для чтения).*

### 3. Ручная проверка через Hive-клиент
Вы также можете вручную проверить данные через Beeline. Имя таблицы берется из конфигурационного JSON-файла QA (поле `"table"`):
```bash
beeline -u "jdbc:hive2://team-37-en:10000/default" -n team
```
Выполните запрос:
```sql
SELECT * FROM <имя_таблицы_из_json> LIMIT 10;
```
