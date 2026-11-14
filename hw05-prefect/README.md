# HW5: Prefect Pipeline

## Описание
Реализован процесс оркестрации данных с помощью Prefect. Flow запускает PySpark-приложение (из HW4), которое работает под управлением YARN, читает данные из HDFS, выполняет трансформации и сохраняет результат.

## Установка Prefect
Для изоляции зависимостей Prefect установлен в виртуальное окружение:
```bash
python3 -m venv venv
source venv/bin/activate
pip install prefect
```

## Запуск сервера Prefect
Сервер запущен в фоновом режиме и доступен на порту 4200:
```bash
nohup prefect server start --host 0.0.0.0 > prefect.log 2>&1 &
```

## Запуск Flow
Для запуска пайплайна выполните:
```bash
source venv/bin/activate
python flow.py
```

## Проверка
Список запусков Flow и их статусы:
```bash
prefect flow-run ls
```
*Ожидаемый результат: последний запуск должен быть со статусом `Completed`.*
