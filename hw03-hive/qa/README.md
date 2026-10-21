# HW3 — Network + QA для Hive

Скрипты **не устанавливают Hive и не загружают данные**. Проверки только читают данные через Beeline; туннель открывает порт только на `127.0.0.1` ноутбука.

## Локальные тесты

Из корня репозитория:

```bash
python3 -m unittest discover -s hw03-hive/qa/tests -v
python3 -m py_compile hw03-hive/qa/check_hive.py
bash -n hw03-hive/qa/open_hive_tunnel.sh
```

## Доступ к HiveServer2 с ноутбука

В ветке `hw3-hive` HiveServer2 предусмотрен на edge (`team-37-en:10000`). После его запуска:

```bash
bash hw03-hive/qa/open_hive_tunnel.sh \
  --edge team@178.236.27.186 \
  --identity-file ~/.ssh/id_ed25519_team37 \
  --remote-host 127.0.0.1 --remote-port 10000
```

Терминал с туннелем останется занят; закрытие — `Ctrl+C`. Можно проверить команду без подключения с `--dry-run`.

Если Beeline установлен на ноутбуке, в другом терминале:

```bash
python3 hw03-hive/qa/check_hive.py \
  --jdbc-url 'jdbc:hive2://127.0.0.1:11000/default' \
  --user team --database default --table books \
  --partition-column year --min-rows 3 \
  --expect-partition year=2022 --expect-partition year=2023
```

Если Beeline есть только на edge, перенесите QA-скрипт в личный каталог edge и запустите там с `--jdbc-url 'jdbc:hive2://127.0.0.1:10000/default'` (без туннеля).

Пример `books(id,title,author,genre)` с партицией `year` взят из ветки `hw3-hive` на 21.10.2026. Если автор загрузки поменяет имена или годы, передайте актуальные значения параметрами CLI.

## Выходные коды

- `0 / PASS`: SQL, строки и заданные непустые партиции проверены.
- `1 / FAIL`: ошибка запроса, соединения или проверяемых данных.
- `2 / NOT TESTED`: Beeline отсутствует на машине запуска.

Проверка одновременных клиентов закреплена за участником №3; metastore и установка Hive — за участником №1. Эти файлы не проверяют их код.
