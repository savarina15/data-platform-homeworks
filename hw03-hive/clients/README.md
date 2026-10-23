# HW3: несколько клиентов Hive одновременно

Hive работает не в embedded-режиме: метаданные в MySQL, клиенты подключаются
к HiveServer2 (`team-37-en:10000`). Поэтому клиентов может быть несколько сразу.

`parallel_clients.sh` одновременно запускает несколько `beeline` (по умолчанию 2),
каждый под своим пользователем, и выполняет запросы к таблице `books`.
`RESULT: PASS` — все клиенты отработали без ошибок и в одно время.

## Запуск

На edge из корня репозитория (Hive развёрнут, данные загружены):

```bash
bash hw03-hive/clients/parallel_clients.sh
CLIENTS=3 bash hw03-hive/clients/parallel_clients.sh   # три клиента
```

Логи клиентов — в `/tmp/hive-clients`.

## Вручную

В двух терминалах на edge:

```bash
beeline -u jdbc:hive2://team-37-en:10000/default -n client1
beeline -u jdbc:hive2://team-37-en:10000/default -n client2
```

и в обоих выполнить `SELECT year, COUNT(*) FROM books GROUP BY year;`.