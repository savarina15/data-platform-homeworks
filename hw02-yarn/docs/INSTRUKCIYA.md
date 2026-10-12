# Настройка и проверка YARN

RM и JobHistory запускаются на `namenode`, три NM — на `datanodes`.
Нужны работающий HDFS, Java 11, Hadoop 3.4.0, Ansible и внутренний SSH-доступ.
Все команды ниже выполняются из корня репозитория на edge.

## Параметры и защита HDFS

`config/nastroiki.example.yml`: `rezhim` выбирает действие, `rezerv_*` задают
резерв RAM для ОС и служебных процессов. Лимит контейнеров округляется вниз до
256 МБ: `min(cap, floor((RAM - reserve) / 256) * 256)`.
Для текущих VM: master 1024 МБ/1 vcore, workers по 1536 МБ/2 vcore;
4096 МБ суммарно вместо 3072 МБ при одинаковых лимитах. Это ёмкость, не замер скорости.
Heap YARN/JHS — 256 МБ; проверка физической памяти контейнеров включена.

Настройки YARN находятся отдельно в `/home/team/yarn/conf`, логи и PID — рядом.
Исходные HDFS XML копируются, оригиналы не редактируются. Форматирование,
остановка HDFS и удаление данных отсутствуют. Добавляются только недостающие
HDFS-каталоги под `/user/team/yarn` для истории, staging и агрегированных логов.
Чужие YARN-демоны не управляются. Изменение настроек работающего YARN отклоняется.

## Запуск

Используйте локальный `shared/inventory.ini` из HW1; ключи и inventory не коммитить.
Для другого стенда изменить inventory и добавить `-e @/path/local-vars.yml`.

```bash
ansible-playbook -i shared/inventory.ini hw02-yarn/automation/cluster.yml --syntax-check
ansible-playbook -i shared/inventory.ini hw02-yarn/automation/cluster.yml
# После сообщения команде о занятии стенда:
ansible-playbook -i shared/inventory.ini hw02-yarn/automation/cluster.yml -e rezhim=deploy
```

Без `-e` действует `plan`: только чтение и показ лимитов. Остальные действия:
`configure` — конфиги, `start` — запуск, `stop` — остановка, `check` — проверки.
`deploy` объединяет configure/start/check. Повторить deploy для проверки
идемпотентности; перед сменой настроек выполнить `-e rezhim=stop`.
Автозапуск при reboot не установлен: после готовности HDFS выполнить start.

## Проверка и сдача

Автоматически проверяются три RUNNING NM с правильными адресами, здоровье NM,
REST JobHistory, YARN RPC и HDFS report до и после запуска. Для небольшой задачи
после согласования со стендом:

```bash
source /home/team/yarn/conf/hadoop-env.sh
hadoop jar "$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.0.jar" pi 1 10
```

Ожидается успешное приложение и строка `Estimated value of Pi is`.
Перед QA участника D также выбрать это окружение; NM адреса в JSON должны
совпадать с hostname inventory и портом 8042, JobHistory обязателен.
Для Team 37 открыть отдельный CMD Windows и оставить туннель работающим:

```cmd
ssh -N -T -i "%USERPROFILE%\.ssh\mts" -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 -L 127.0.0.1:18088:team-37-nn:8088 -L 127.0.0.1:18042:team-37-nn:8042 -L 127.0.0.1:28042:team-37-00:8042 -L 127.0.0.1:38042:team-37-01:8042 -L 127.0.0.1:29888:team-37-nn:19888 team@178.236.27.186
```

В браузере использовать `127.0.0.1` и локальные порты 18088, 18042, 28042,
38042, 29888 соответственно; Ctrl-C закрывает туннель. Для другого стенда
заменить ключ, edge-адрес и целевые узлы. Подробные туннели — в `QA-NETWORK.md`.
Проверить браузером RM `/cluster` (8088), каждый NM `/node` (8042),
JHS `/jobhistory` (19888); старт туннеля сам по себе не доказывает доступность UI.
Не открывать административные порты в интернет. Логи — `/home/team/yarn/logs`,
контейнерные — `container-logs`, агрегированные — `yarn logs -applicationId <ID>`.
Сохранить check, UI, логи текущего запуска, повторный deploy и проверку HDFS;
оформить PR, получить ревью владельца HDFS и проверить QA-блок участника D.
