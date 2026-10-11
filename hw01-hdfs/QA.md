# HW1: проверка HDFS

Эти проверки относятся к зоне Network / QA / Documentation. Они не устанавливают
Hadoop, не форматируют NameNode и не запускают демоны. Реальный кластер в ходе
разработки этих файлов **NOT TESTED**: SSH и изменения общего стенда не разрешены.

## Подготовка и конфигурация

Запускать `scripts/check-hdfs.sh` из копии репозитория на edge или другом узле,
имеющем прямой доступ к RPC NameNode, DataNode и внутренним HTTP-интерфейсам.
Нужны Bash, Python 3 (только стандартная библиотека), curl, Java 11 и Hadoop 3.4.0.
Учетная запись должна иметь право на `dfsadmin -report` и чтение всего дерева для
`fsck /`. Отказ в правах не означает исправность кластера.

CLI выбирается из `$HADOOP_HOME/bin`, если переменная задана, иначе из `PATH`.
`JAVA_HOME` и `HADOOP_CONF_DIR` передаются без изменений. Скрипт не читает и не
исполняет Ansible YAML/Jinja: это шаблоны развёртывания, а не клиентская конфигурация.
Пример окружения согласно `origin/infra/setup` (проверьте реальные пути):

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/home/team/hadoop-3.4.0
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
```

Из корня репозитория подготовьте отдельный QA-конфиг:

```bash
cp shared/qa/cluster.example.json /tmp/team37-qa.json
```

В `/tmp/team37-qa.json` замените `nn.example.internal`,
`worker00.example.internal`, `worker01.example.internal` на действительные
имена. Для текущего HDFS inventory это `team-37-nn`, `team-37-00`, `team-37-01`.
Пример намеренно не является подтверждением развернутых сервисов. Схема и
источники портов описаны в [инструкции Network](../hw02-yarn/QA-NETWORK.md).
Для HW1 достаточно `services.namenode`, `services.secondarynamenode`, трёх
DataNode URL, списка `datanodes` и `checkpoint_max_age_seconds`.

После разрешённого входа на стенд значения можно сверить read-only командами:

```bash
"$HADOOP_HOME/bin/hdfs" getconf -confKey fs.defaultFS
"$HADOOP_HOME/bin/hdfs" getconf -confKey dfs.namenode.http-address
"$HADOOP_HOME/bin/hdfs" getconf -confKey dfs.namenode.secondary.http-address
"$HADOOP_HOME/bin/hdfs" getconf -confKey dfs.datanode.http.address
"$HADOOP_HOME/bin/hdfs" getconf -confKey dfs.http.policy
"$HADOOP_HOME/bin/hdfs" getconf -confKey dfs.namenode.checkpoint.period
```

Адрес привязки `0.0.0.0` заменяется в QA-конфиге на достижимое имя конкретной VM;
это не повод изменять Hadoop XML. При HTTPS используйте HTTPS-адрес и фактический
порт, доверенный сертификат и имя из сертификата. HTTP-аутентификация/Kerberos,
HA, IPv6 и reverse proxy с префиксом пути не входят в текущий профиль скрипта.
При их появлении сначала адаптируйте проверки; не отключайте защиту.

## Read-only диагностика

```bash
bash hw01-hdfs/scripts/check-hdfs.sh --help
bash hw01-hdfs/scripts/check-hdfs.sh --config /tmp/team37-qa.json --timeout 60
echo "$?"
```

Каждая внешняя команда имеет отдельный таймаут (1–86400 секунд, по умолчанию 60).
Долгий `fsck /` может требовать увеличения `--timeout`; он читает метаданные всего
дерева и создаёт нагрузку, поэтому не следует запускать его непрерывно.

Проверяется:

- CLI Hadoop 3.x и `fs.defaultFS=hdfs://…`, чтобы не тестировать локальную FS;
- RPC NameNode: полный `dfsadmin -report`, ровно 3 разных Live DataNode,
  состояние `Normal`, положительная ёмкость каждого, отсутствие dead,
  decommission/maintenance и ненулевых проблемных счётчиков блоков.
  `Pending deletion blocks` — очередь фонового удаления: ненулевое значение
  выводится как INFO и само по себе не вызывает FAIL;
- `dfsadmin -safemode get`: ожидается OFF, состояние не меняется;
- `fsck /`: явный HEALTHY, отсутствие признаков ошибок, ненулевых missing,
  corrupt, under/mis-replicated blocks; без `-delete` или `-move`;
- Web UI на `/dfshealth.html`, `/status.html` и `/datanode.html`: HTTP 200,
  HTML с признаками соответствующей роли, без страницы входа/поля пароля.
  Для NameNode и DataNode дополнительно проверяются их JMX beans;
- JMX SecondaryNameNode: ответ именно `SecondaryNameNodeInfo` подтверждает
  доступность демона отдельно от выполнения checkpoint;
- возраст checkpoint: `LastCheckpointDeltaMs` в пределах
  `checkpoint_max_age_seconds` (пример: 7200 секунд) даёт PASS. Значение `-1`
  означает, что первый checkpoint ещё не выполнен: выводится
  `NOT TESTED (optional)`, итоговый код не ухудшается и smoke не блокируется.
  Это не утверждение об успешном checkpoint. Отсутствующая метрика даёт обычный
  NOT TESTED (код 2), просроченный/некорректный возраст — FAIL.
  Порог согласуйте с периодом checkpoint и временем его выполнения.
  Принудительный checkpoint не запускается.

Hadoop не печатает пустые секции Dead/maintenance в полном отчёте; их отсутствие
при валидном полном отчёте допустимо. Неизвестный/неполный формат не считается PASS.
Посторонняя страница или Login с HTTP 200 не считается Hadoop UI. Проверяется
штатный интерфейс Hadoop 3.4; изменённый/проксированный HTML может потребовать
адаптации признаков в проверке. RPC, fsck и JMX дают отдельные свидетельства
состояния сервисов. Проверка checkpoint не доказывает возможность восстановления
из резервной копии и не заменяет анализ логов.

## Smoke-test: только с разрешением на изменение HDFS

По умолчанию запись отключена. После отдельного согласования используйте
**существующий личный** каталог, на который у вашего пользователя есть права:

```bash
bash hw01-hdfs/scripts/check-hdfs.sh \
  --config /tmp/team37-qa.json --smoke --smoke-base /user/team
```

`/user/team` здесь пример, его существование не гарантируется. Скрипт не создаёт
родительский каталог. После успешной диагностики создаётся
`<smoke-base>/qa-hdfs-<UUID>` командой `mkdir` без `-p`, выставляются права 700,
записывается маленький `payload`, данные читаются и сравниваются.

Очистка в `finally` выполняется также при ошибке, Ctrl-C и SIGTERM: удаляется
только `payload` внутри успешно созданного этим запуском каталога, затем пустой
каталог через `rmdir`. Нет рекурсивного удаления, glob или удаления общего родителя.
При коллизии имени/неподтверждённом результате `mkdir` ничего не удаляется.
Это эквивалент cleanup через `trap` для Python-реализации.

При SIGKILL, потере питания/сети или таймауте `mkdir` может остаться UUID-каталог;
автоматически удалять его следующий запуск не будет. Проверьте путь из сообщения,
владельца и содержимое вручную перед согласованной очисткой. Ошибка cleanup даёт
FAIL, а не успешный smoke-test. Каталог должен принадлежать вам: протокол не
предназначен для защиты от другого процесса, работающего под той же учётной записью.

## Результаты и диагностика

| Результат | Значение |
|---|---|
| `PASS`, exit 0 | Все обязательные проверки данного профиля прошли |
| `FAIL`, exit 1 | Команда/HTTP недоступны, плохое состояние, неизвестный ответ или ошибка очистки |
| `NOT TESTED`, exit 2 | Нет зависимости, конфигурации или метрики, необходимой для проверки; ошибки аргументов также 2 |
| exit 130 | Проверка прервана |

При одновременных FAIL и NOT TESTED итоговый код 1. Отключённый smoke выводит
`NOT TESTED (optional)` и не меняет код read-only проверки. Exit 0 без `--smoke`
**не подтверждает запись/чтение**. Итоговый PASS не является оценкой всего ДЗ.
Ожидание первого checkpoint также отображается отдельно и не означает
неисправность работающего HDFS.

| Проблема | Что проверить без исправления данных |
|---|---|
| CLI отсутствует / Java не запускается | `HADOOP_HOME`, `JAVA_HOME`, наличие Java 11 и разрешения файлов |
| `file:///` вместо HDFS | `HADOOP_CONF_DIR`, установлен ли `core-site.xml` владельцем HDFS |
| RPC timeout / connection refused | имя NameNode, DNS/hosts, RPC-порт и состояние сервиса у владельца |
| DataNode меньше 3 / dead / ёмкость 0 | отчёт, диски, доступность узла и логи соответствующего DataNode |
| Safe mode ON | причина в логах NameNode; не применять принудительный `leave` автоматически |
| fsck не HEALTHY | сохранить полный `hdfs fsck /` и `dfsadmin -report`, передать владельцу HDFS |
| первый checkpoint ещё не выполнен | дождаться штатного периода; доступность SecondaryNameNode проверяется отдельно |
| метрика checkpoint отсутствует / checkpoint устарел | проверить версию/ответ JMX; передать владельцу время запуска, период и логи |
| HTTP 401/403/302 | аутентификация, правильный URL, редирект; не считать страницу логина исправностью сервиса |
| cleanup FAIL | проверить только выведенный UUID-путь; не удалять общий каталог |

Отсутствие критических ошибок в логах автоматически **не подтверждается**.
После разрешения на доступ на каждом узле роли изучите логи за интервал запуска
и проверок. В `infra/setup` каталог задаётся `hadoop_log_dir` (пример
`/home/team/hdfs/logs`). Команда поиска кандидатов:

```bash
rg -n 'ERROR|FATAL|Exception|Incompatible clusterIDs|OutOfMemoryError' /home/team/hdfs/logs
```

Ненулевой код `rg` может означать отсутствие совпадений (1) либо ошибку чтения (2).
Совпадение само по себе не доказывает критическую ошибку: нужны время, контекст,
связь с работающим демоном. Без чтения логов всех ролей требование нельзя закрыть.

## Проверки разработки и источники

Локальные тесты без подключения к стенду:

```bash
python3 -m unittest discover -s shared/qa/tests -v
bash -n hw01-hdfs/scripts/check-hdfs.sh
bash -n hw02-yarn/scripts/check-yarn.sh
bash -n hw02-yarn/scripts/open-web-ui.sh
git diff --check
```

Тесты используют mock CLI/curl/ssh, включая аварии записи и очистки. Они не
являются интеграционными тестами Hadoop. Отчёт и подготовка PR —
[shared/qa/REVIEW.md](../shared/qa/REVIEW.md).

Основания: [HDFS Commands 3.4.0](https://hadoop.apache.org/docs/r3.4.0/hadoop-project-dist/hadoop-hdfs/HDFSCommands.html),
[SecondaryNameNode 3.4.0: JMX и checkpoint](https://github.com/apache/hadoop/blob/rel/release-3.4.0/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/SecondaryNameNode.java).
