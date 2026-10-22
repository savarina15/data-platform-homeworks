# HW4: Apache Spark под управлением YARN

## Описание
Развернут Apache Spark 3.5.1 для работы под управлением YARN. Реализована задача, которая читает данные из Hive (таблица `books`), выполняет трансформацию (фильтрация по году и агрегация по жанрам), сохраняет результат в Hive с партиционированием.

## Архитектура
- **Spark Master**: YARN ResourceManager (team-37-nn)
- **Hive Metastore**: team-37-en (порт 9083)
- **HDFS**: team-37-nn (порт 9000)

## Установка (Автоматизированная)
Для установки Spark используется Ansible-плейбук:
```bash
cd ~/data-platform-homeworks
ansible-playbook -i shared/inventory.ini hw04-spark/ansible/install-spark.yml
