# HW3: Apache Hive (Non-Embedded)

## Описание
Развернут Apache Hive 4.0.0-alpha-2 с внешним MySQL Metastore (не embedded), что позволяет подключаться нескольким клиентам одновременно. Реализована автоматизированная загрузка данных о литературе в партиционированную таблицу.

## Архитектура
- Hive Metastore: team-37-en (порт 9083)
- HiveServer2: team-37-en (порт 10000)
- Metastore DB: MySQL 8.0 на team-37-en (база `metastore`)

## Установка
Для развертывания Hive используется Ansible-плейбук:
```bash
cd ~/data-platform-homeworks
ansible-playbook -i shared/inventory.ini hw03-hive/ansible/install-hive.yml
```

## Загрузка данных

[`scripts/load_data.sh`](scripts/load_data.sh) создаёт тестовые CSV через `echo`,
загружает их в HDFS и выполняет встроенный SQL через Beeline. Создаётся таблица
`default.books` с партициями по году; повторный запуск заменяет учебные партиции.
В конце скрипт выводит партиции, число строк по годам и все три книги.
Исходные CSV сохраняются в HDFS для HW4.
