# ДЗ 5

Prefect запускает `run_on_yarn.sh`. Spark работает через YARN, читает CSV из HDFS (`/user/team/datasets/books`), приводит типы и считает книги за 2023 год по жанрам. Результат пишется в Parquet (`/user/team/hw5/books_stats`) и доступен в Hive как `default.hw5_books_stats`.

Команды выполняются на `team-37-en` из корня репозитория. Если Prefect ещё не установлен, сначала настрой окружение.

```bash
python3 -m venv ~/prefect-hw5/venv
~/prefect-hw5/venv/bin/python -m pip install 'prefect>=3,<4'
```

Если сервер Prefect выключен, запусти его в отдельной SSH-сессии.

```bash
~/prefect-hw5/venv/bin/prefect server start --host 127.0.0.1 --port 4200
```

Запусти Flow и проверь таблицу.

```bash
export PREFECT_API_URL=http://127.0.0.1:4200/api
curl -fsS --max-time 5 "$PREFECT_API_URL/health" &&
  ~/prefect-hw5/venv/bin/python hw05-prefect/flow.py

~/hive/bin/beeline -u 'jdbc:hive2://team-37-en:10000/default' -n team \
  -e 'SELECT genre, `count` FROM default.hw5_books_stats ORDER BY genre'
```

19.11.2026 Prefect показал `Completed`, а YARN — `FINISHED / SUCCEEDED`.
Hive вернул `Dystopia — 1` и `Fiction — 1`.

- Prefect `flow_run_id` — `c6e03656-688c-4958-9211-f89ba7af5fb4`
- YARN `application_id` — `application_1792582217685_0010`
