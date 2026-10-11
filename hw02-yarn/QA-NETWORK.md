# HW2: YARN QA и доступ к Web UI

## Границы реализации

`scripts/check-yarn.sh` проверяет готовый YARN; он не разворачивает его и не запускает
jobs. `scripts/open-web-ui.sh` создаёт SSH local forwarding с ноутбука через edge.
Оба используют общий модуль `shared/qa/check_cluster.py` и JSON с явными адресами.
Python 3 нужен для безопасного разбора JSON, проверки аргументов, обработки процессов
и таймаутов. Дополнительных Python-пакетов нет.

По официальному уточнению преподавателя для HW2 достаточно Web UI через
**localhost**. SSH-туннели удовлетворяют этому способу доступа; публичные URL,
Nginx и открытие административных портов в интернет не нужны.

Пример из трёх NodeManager — шаблон QA-конфигурации, а не подтверждение фактических
ролей. YARN не обязан иметь столько же рабочих узлов, сколько HDFS:
в `nodemanagers` укажите согласованный с владельцем YARN список.
JobHistory указан в README HW2 как проектное требование, поэтому в примере он
обязателен. Если команда явно исключит его из реализации/сдачи, установите
`jobhistory_required: false` и зафиксируйте обоснование; его отсутствие нельзя
скрывать только ради PASS.

## Единая конфигурация QA и туннелей

Из корня репозитория скопируйте `shared/qa/cluster.example.json` в
`/tmp/team37-qa.json` и отредактируйте. Секретов в нём быть не должно.
Встроенных IP team-37 в коде нет. Конфигурация независима от Ansible и не меняет
настройки Hadoop; соответствие с развёрнутыми XML проверяется вручную.

| Поле | Назначение |
|---|---|
| `services.<имя>.url` | Корень `http(s)://имя:порт`; порт обязателен, без credentials/пути/query |
| `services.<имя>.local_port` | Локальный порт ноутбука для туннеля, не нужен для QA |
| `datanodes` | Три ключа `services` для UI HDFS |
| `nodemanagers` | Непустой список ключей `services`; его длина определяет ожидаемое число NM |
| `jobhistory_required` | Явное `true` или `false` по проектному решению |
| `checkpoint_max_age_seconds` | Допустимый возраст завершённого checkpoint; по умолчанию 7200 |

Имена `namenode`, `secondarynamenode`, `resourcemanager`, `jobhistory` фиксированы
для соответствующих проверок; ключи DataNode/NodeManager выбираются произвольно.
Число портов 1–65535. IPv4 и DNS поддерживаются, IPv6 в текущем профиле не поддержан.
Неизвестные поля, дубликаты групп и некорректные URL отклоняются до сетевых запросов.

| Интерфейс | Источник адреса/порта | Значение в примере, не гарантия |
|---|---|---|
| NameNode | `dfs.namenode.http-address`, Ansible `namenode_http_port` | 9870 |
| SecondaryNameNode | `dfs.namenode.secondary.http-address`, `secondarynamenode_http_port` в `infra/setup` | 9868 |
| DataNode, каждый | `dfs.datanode.http.address` | 9864 |
| ResourceManager | `yarn.resourcemanager.webapp.address`, `resourcemanager_webapp_port` | 8088 |
| NodeManager, каждый | `yarn.nodemanager.webapp.address`, `nodemanager_webapp_port` | 8042 |
| JobHistory | `mapreduce.jobhistory.webapp.address`, `jobhistory_webapp_port` | 19888 |

При HTTPS смотрите соответствующие HTTPS properties и HTTP policy в развёрнутых
XML. Не заменяйте адреса привязки сервера на публичные ради этих скриптов.
Для team-37 NameNode/SecondaryNameNode/DataNode находятся на `team-37-nn`, ещё два
DataNode на `team-37-00` и `team-37-01`. Места YARN-ролей пока нужно подтвердить.
NodeManager URL должен содержать ровно имя/порт из `nodeHTTPAddress` ResourceManager;
замена имени на эквивалентный IP приводит к явному несовпадению, а не к молчаливому
принятию другого набора узлов.

## Проверка YARN внутри сети

Нужны Bash, Python 3, curl, Java/Hadoop и установленная клиентская конфигурация
YARN на edge или другом доверенном узле. Используйте то же окружение, что описано
в [HW1 QA](../hw01-hdfs/QA.md). Команды из корня репозитория после разрешённого
входа на стенд:

```bash
bash hw02-yarn/scripts/check-yarn.sh --help
bash hw02-yarn/scripts/check-yarn.sh --config /tmp/team37-qa.json --timeout 60
echo "$?"
```

Проверки: версия CLI; `yarn node -list -all` через RPC; точное число разных NodeId,
все в RUNNING и ожидаемые HTTP-адреса; HTTP UI ResourceManager; REST cluster/info
со STARTED и ACTIVE (если поле HA присутствует); REST cluster/nodes с полным
набором RUNNING-узлов; UI и `nodeHealthy=true` каждого NodeManager; UI и REST info
JobHistory, когда он обязателен. LOST, UNHEALTHY, DECOMMISSIONED и другие состояния
не игнорируются. Запуск application не производится.

Коды: 0 — PASS обязательных проверок, 1 — FAIL, 2 — NOT TESTED/ошибка аргументов,
130 — прерывание. FAIL имеет приоритет над NOT TESTED. Отсутствующие зависимости
или список NodeManager дают NOT TESTED; connection refused/timeout работающей
команды дают FAIL. Это ожидаемо до развёртывания YARN и не доказывает дефект QA.
Неверный HTTP JSON, страница входа вместо API, 401/403 и редиректы не считаются PASS.
Дополнительно проверяется HTML штатных UI: `/cluster` для RM, `/node` для NM,
`/jobhistory` для JHS. HTTP 200 с посторонней страницей или Login даёт FAIL,
даже если REST отвечает корректно. Проверка HTML рассчитана на штатный Hadoop 3.4;
брендированный интерфейс может требовать адаптации признаков.
Аутентифицированный HTTP/SPNEGO профиль ещё не реализован. TLS-проверка не отключается;
можно передать доверенный CA через `CURL_CA_BUNDLE`.

## SSH-туннели с ноутбука

На ноутбуке нужны Bash, Python 3 и OpenSSH client; Hadoop/Java не требуются.
Имена внутренних VM должны разрешаться **на edge**; подключение к целевым портам
выполняет edge. Нужны действующая SSH-учётная запись и разрешённый forwarding.
Ключ укажите через `--identity-file` либо используйте уже настроенный SSH
config/agent. Скрипт проверяет существование и читаемость файла, передаёт путь
в `ssh -i` отдельным аргументом и не читает/копирует его содержимое.
Публичный адрес edge team-37: `178.236.27.186`.

Сначала безопасная проверка команды, без SSH:

```bash
bash hw02-yarn/scripts/open-web-ui.sh \
  --config /tmp/team37-qa.json --edge team@178.236.27.186 \
  --identity-file "$HOME/.ssh/team37_external" \
  --service namenode --service secondarynamenode \
  --service resourcemanager --service jobhistory --dry-run
```

`team` — пример из inventory, подтвердите внешнего SSH-пользователя. `--ssh-port`
позволяет задать нестандартный порт (по умолчанию 22). Для SSH config alias всё
равно используйте `--edge пользователь@alias`.
`team37_external` — пример произвольного имени вашего **внешнего** ключа,
замените путь на существующий файл. Поддерживаются относительные пути, `~`
и пробелы (путь берите в кавычки). С `--identity-file` включается
`IdentitiesOnly=yes`: SSH предлагает заданные ключи, а не все ключи агента.
Файл проверяется и при dry-run. Права и passphrase обрабатывает OpenSSH;
скрипт не меняет разрешения файла. Если используете SSH config/agent,
просто опустите `--identity-file`.

После отдельного разрешения на подключение запустите ту же команду без `--dry-run`.
Для всех вспомогательных UI добавьте нужные сервисы:

```bash
bash hw02-yarn/scripts/open-web-ui.sh \
  --config /tmp/team37-qa.json --edge team@178.236.27.186 \
  --identity-file "$HOME/.ssh/team37_external" \
  --service namenode --service secondarynamenode \
  --service datanode-nn --service datanode-00 --service datanode-01 \
  --service resourcemanager \
  --service nodemanager-nn --service nodemanager-00 --service nodemanager-01 \
  --service jobhistory
```

Туннели работают на переднем плане; Ctrl-C закрывает соединение. Пример печатает
URL `http://127.0.0.1:19870`, `:18088` и другие из `local_port`. Откройте каждый
выведенный URL. Привязка только к `127.0.0.1`, без `-g`; agent/X11 forwarding
отключены; проверка host key обязательна; keepalive выявляет потерю edge.
Новый host key сначала сверьте по доверенному каналу и добавьте штатной процедурой
команды — не отключайте `StrictHostKeyChecking` и не доверяйте одному `ssh-keyscan`.
Используется доверенный SSH config пользователя, его настройки не изменяются.

Для прямого открытия штатной страницы добавьте к локальному URL путь:
NameNode `/dfshealth.html`, SecondaryNameNode `/status.html`, DataNode
`/datanode.html`, ResourceManager `/cluster`, NodeManager `/node`, JobHistory
`/jobhistory`. Например: `http://127.0.0.1:19870/dfshealth.html` и
`http://127.0.0.1:18088/cluster` для портов из примера.

Успешный старт SSH доказывает создание локальных слушателей, **не** доступность
целевых UI: OpenSSH проверяет целевой порт при обращении к нему. Проверяйте браузер
или curl отдельно. Скрипт передаёт ненулевой код SSH (обычно 255), например при
занятом порте, отказе аутентификации или неизвестном host key. Dry-run возвращает 0
только как успешное построение команды и печатает NOT TESTED для соединения.

Ссылки внутри UI могут вести на внутренние hostname/порты. Открывайте отдельный
локальный URL соответствующего демона; скрипт не переписывает HTML, redirects,
WebHDFS download или адреса ApplicationMaster. Для демонстрации требуемых UI
открывайте каждый локальный URL отдельно. При HTTPS локальный IP
обычно не совпадает с SAN сертификата: используйте согласованное DNS-имя и
доверенный CA; не отключайте проверку сертификата.

## Диагностика и оставшиеся проверки

| Симптом | Действие |
|---|---|
| CLI не видит RM | Проверить `HADOOP_CONF_DIR`, `yarn-site.xml`, имя/порт RM с владельцем YARN |
| CLI/REST показывают разные узлы | Проверить, что клиент и QA URL указывают на один кластер |
| NM unhealthy | Проверить `healthReport`, диски, права локальных каталогов и логи NM |
| JobHistory FAIL | Уточнить, развернут ли он; проверить mapred-site.xml и логи |
| `Address already in use` | Выбрать другой `local_port`; скрипт не завершает чужие процессы |
| `administratively prohibited` | Передать владельцу SSH; самостоятельно не менять sshd_config |
| `channel ... connect failed` | Проверить DNS на edge, фактический HTTP bind/порт и готовность демона |
| Главная страница открылась, ссылки нет | Внутренние абсолютные URL: открыть туннель нужного сервиса |

До сдачи нужны реальные проверки обоих QA-скриптов, разрешённый smoke-test HDFS,
проверка всех UI с ноутбука, чтение логов всех ролей, подтверждение количества NM
в QA-конфигурации. Публичная публикация для сдачи не требуется.
Вычислительные jobs требуют отдельного
согласования. Локальные mock-тесты не доказывают планирование/исполнение контейнеров.

Источники: [YARN Commands 3.4.0](https://hadoop.apache.org/docs/r3.4.0/hadoop-yarn/hadoop-yarn-site/YarnCommands.html),
[ResourceManager REST](https://hadoop.apache.org/docs/r3.4.0/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html),
[NodeManager REST](https://hadoop.apache.org/docs/r3.4.0/hadoop-yarn/hadoop-yarn-site/NodeManagerRest.html),
[JobHistory REST](https://hadoop.apache.org/docs/r3.4.0/hadoop-mapreduce-client/hadoop-mapreduce-client-hs/HistoryServerRest.html).
