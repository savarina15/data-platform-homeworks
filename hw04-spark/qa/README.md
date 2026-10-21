# HW4 — Spark → Hive: интеграция и проверка результата

Зона ответственности Network / Integration / QA. Этот модуль **не устанавливает Spark/Hive, не запускает Spark-задачу и не переписывает результат обработки**: это задачи других участников. Он регистрирует **готовый** партиционированный Parquet-результат Spark во внешней таблице Hive и проверяет чтение обычным Beeline-клиентом.

## Контракт с PySpark-приложением

Пример `spark_hive.example.json` описывает демонстрационный результат:
- Spark сохраняет Parquet в `/user/team/spark/books_summary`.
- Колонки файлов: `genre` (STRING), `books_count` (BIGINT).
- Партиционирование по `year` (INT): каталоги `year=2022/`, `year=2023/`.
- Выходной путь должен быть каталогом HDFS с файлам `part-*.parquet`, а не локальным каталогом ноутбука.

**Это пример, а не подтверждённая схема однокурсника.** Когда автор Spark определит окончательную схему и каталог, обновите копию JSON *в соответствии с его результатом*. Поле `columns` содержит **только физические колонки Parquet, без `year`**, поскольку партиция добавляется через `PARTITIONED BY`. Не запускайте `apply`, пока Spark не создал ожидаемый результат. Одинаковая таблица и LOCATION должны использоваться при повторных запусках; `CREATE IF NOT EXISTS` не меняет схему/LOCATION уже существующей таблицы.

## Локальная подготовка

Из корня репозитория:

```bash
python3 -m unittest discover -s hw04-spark/qa/tests -v
python3 -m py_compile hw04-spark/qa/hive_integration.py
cp hw04-spark/qa/spark_hive.example.json /tmp/team37-spark-hive.json
python3 hw04-spark/qa/hive_integration.py ddl --config /tmp/team37-spark-hive.json
```

`ddl` только печатает SQL и ничего не изменяет в Hive. Проверки подключения к HiveServer2 и SSH-туннель для ноутбука уже находятся в `hw03-hive/qa/`: используйте их, не создавайте второй туннель на тот же порт.

## Когда Spark успешно записал Parquet

Нужен установленный Beeline, доступ к HiveServer2 и подготовленная копия JSON. С ноутбука можно подключаться по туннелю HW3 (`jdbc:hive2://127.0.0.1:11000/default`); с edge, если HiveServer2 слушает порт 10000, замените JDBC URL в копии JSON на `jdbc:hive2://127.0.0.1:10000/default`.

**Сначала** прочитайте DDL и убедитесь, что столбцы, типы, партиция и LOCATION соответствуют *фактическому* результату Spark.

```bash
python3 hw04-spark/qa/hive_integration.py ddl --config /tmp/team37-spark-hive.json
```

**Затем** зарегистрируйте внешнюю таблицу и партиции (изменяет только Hive Metastore; не перезаписывает Parquet в HDFS):

```bash
python3 hw04-spark/qa/hive_integration.py apply --config /tmp/team37-spark-hive.json
```

**Повторная проверка без записи:**

```bash
python3 hw04-spark/qa/hive_integration.py verify --config /tmp/team37-spark-hive.json
```

`apply` выполняет `CREATE EXTERNAL TABLE IF NOT EXISTS ... STORED AS PARQUET LOCATION ...` и `MSCK REPAIR TABLE`; затем проверяет `COUNT(*)`, наличие и непустоту ожидаемых партиций, а также реальное чтение столбца из Parquet через Beeline. `verify` выполняет только SQL-запросы чтения.

## Результаты

- `RESULT: PASS; exit=0` — успешно зарегистрировано/прочитано (для `apply`) либо проверено (для `verify`).
- `FAIL`, exit 1 — ошибка SQL, схема/данные не совпали, недостаточно строк или партиций.
- `NOT TESTED`, exit 2 — Beeline не найден. Команда `ddl` всегда остаётся *локальным предварительным просмотром* и не подтверждает работоспособность кластера.

Не делайте вывод, что вся HW4 готова по этому тесту: отдельные доказательства запуска Spark под YARN, чтения HDFS, преобразований и партиционированной записи предоставляет команда.
