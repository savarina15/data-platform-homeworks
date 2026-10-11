#!/usr/bin/env python3
"""Read-only Hadoop QA, opt-in owned-file smoke test, and loopback SSH tunnels.

Python's standard library handles JSON, argv, timeouts and byte comparisons;
no shell evaluation, third-party packages, or Hadoop configuration writes.
"""

import argparse
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
from urllib.parse import urlsplit
import uuid


class Failed(Exception):
    pass


class NotTested(Exception):
    pass


class OptionalNotTested(NotTested):
    """An explicitly reported observation that does not invalidate readiness."""


class Page(HTMLParser):
    """Inspect rendered text, ignoring comments and script/style contents."""

    def __init__(self):
        super().__init__()
        self.text = []
        self.hidden = 0
        self.html = False
        self.password = False

    def handle_starttag(self, tag, attrs):
        if tag == "html":
            self.html = True
        if tag in ("script", "style"):
            self.hidden += 1
        if tag == "input" and dict(attrs).get("type", "").lower() == "password":
            self.password = True

    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            self.hidden = max(0, self.hidden - 1)

    def handle_data(self, data):
        if not self.hidden:
            self.text.append(data)


# Use actual UI pages, not HDFS index.html stubs containing a browser redirect.
WEB_PAGES = {
    "namenode": ("/dfshealth.html", r"\bnamenode\b"),
    "secondarynamenode": ("/status.html", r"\bsecondary\s*namenode\b"),
    "datanode": ("/datanode.html", r"\bdatanode\b"),
    "resourcemanager": ("/cluster", r"(?s)(?=.*\b(?:resource\s*manager|cluster)\b)(?=.*\bapplications\b)"),
    "nodemanager": ("/node", r"\bnode\s*manager\b"),
    "jobhistory": ("/jobhistory", r"\b(?:job\s*history|history\s*server)\b"),
}


def require(condition, message):
    if not condition:
        raise Failed(message)


def positive(value):
    number = int(value)
    if not 1 <= number <= 86400:
        raise argparse.ArgumentTypeError("ожидается целое число 1..86400")
    return number


def port(value):
    if type(value) is not int or not 1 <= value <= 65535:
        raise ValueError("порт должен быть целым числом 1..65535")
    return value


def endpoint(value):
    if not isinstance(value, str):
        raise ValueError("URL должен быть строкой")
    url = urlsplit(value)
    if (url.scheme not in ("http", "https") or not url.hostname
            or url.username or url.password or url.query or url.fragment
            or url.path not in ("", "/") or not url.port
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", url.hostname)
            or url.hostname == "0.0.0.0"):
        raise ValueError("нужен http(s)://DNS-имя-или-IPv4:порт без credentials и пути")
    port(url.port)
    return url


def load_config(path):
    if not path:
        return {}
    config = json.loads(Path(path).read_text())
    if not isinstance(config, dict):
        raise ValueError("конфигурация должна быть JSON object")
    allowed = {"services", "datanodes", "nodemanagers", "jobhistory_required",
               "checkpoint_max_age_seconds"}
    if set(config) - allowed:
        raise ValueError("неизвестные поля конфигурации: " + str(set(config) - allowed))
    services = config.get("services", {})
    if not isinstance(services, dict):
        raise ValueError("services должен быть object")
    for name, service in services.items():
        if not re.fullmatch(r"[a-z][a-z0-9-]*", name) or not isinstance(service, dict):
            raise ValueError("некорректное описание сервиса")
        if set(service) - {"url", "local_port"} or "url" not in service:
            raise ValueError("сервис поддерживает только url и local_port")
        endpoint(service["url"])
        if "local_port" in service:
            port(service["local_port"])
    for group in ("datanodes", "nodemanagers"):
        if group in config:
            members = config[group]
            if (not isinstance(members, list) or not members
                    or any(not isinstance(x, str) for x in members)
                    or len(set(members)) != len(members)
                    or any(x not in services for x in members)):
                raise ValueError(f"{group}: нужен непустой список разных ключей services")
            urls = [services[x]["url"].rstrip("/") for x in members]
            if len(set(urls)) != len(urls):
                raise ValueError(f"{group}: повторяются адреса сервисов")
    if "jobhistory_required" in config and type(config["jobhistory_required"]) is not bool:
        raise ValueError("jobhistory_required должен быть true или false")
    age = config.get("checkpoint_max_age_seconds", 7200)
    if type(age) is not int or age <= 0:
        raise ValueError("checkpoint_max_age_seconds должен быть положительным целым")
    return config


def parse_report(output):
    live = re.findall(r"^Live datanodes \((\d+)\):\s*$", output, re.M)
    require(live == ["3"], f"ожидалось ровно 3 Live DataNode, получено {live}")
    for name in ("Dead", "Decommissioning", "Entering maintenance", "In maintenance"):
        counts = re.findall(rf"^{name} datanodes \((\d+)\):\s*$", output, re.M | re.I)
        require(all(int(x) == 0 for x in counts), f"{name} DataNode: {counts}")
    # Hadoop omits zero-sized dead/maintenance sections in its full report.
    states = re.findall(r"^Decommission Status\s*:\s*(.*?)\s*$", output, re.M)
    require(states == ["Normal"] * 3, f"DataNode не в Normal или отчёт неполон: {states}")
    names = re.findall(r"^Name:\s*(\S+)", output, re.M)
    require(len(names) == len(set(names)) == 3, "ожидалось 3 разных DataNode в отчёте")
    capacities = re.findall(r"^Configured Capacity:\s*(\d+)", output, re.M)
    require(len(capacities) == 4 and all(int(x) > 0 for x in capacities),
            "нулевая ёмкость или неполный dfsadmin report")
    for label in (r"(?:Under replicated|Low redundancy) blocks", "Blocks with corrupt replicas", "Missing blocks"):
        require(re.search(rf"^\s*{label}:\s*\d+", output, re.M), "нет обязательного счётчика: " + label)
    counters = re.findall(r"^\s*(?:Under replicated blocks|Blocks with corrupt replicas|"
                          r"Missing blocks|Missing blocks \(with replication factor 1\)|"
                          r"Low redundancy blocks|"
                          r"Low redundancy block groups|Block groups with corrupt internal blocks|"
                          r"Missing block groups|Low redundancy (?:blocks|block groups) with highest priority to recover):\s*(\d+)", output, re.M)
    require(all(int(x) == 0 for x in counters), "ненулевые проблемные block counters: " + str(counters))
    pending = re.findall(r"^\s*Pending deletion blocks:\s*(\d+)", output, re.M)
    if any(int(x) > 0 for x in pending):
        print(f"INFO: Pending deletion blocks: {', '.join(pending)}; очередь удаления, не признак повреждения")


def parse_fsck(output):
    require(re.search(r"^The filesystem under path '/' is HEALTHY\s*$", output, re.M),
            "fsck не подтвердил HEALTHY для /")
    require(not re.search(r"\b(?:CORRUPT|FAILED|UNHEALTHY)\b|(?i:Permission denied)", output),
            "fsck содержит ошибки или недоступные пути")
    for count in re.findall(r"^\s*(?:Under-replicated blocks|Mis-replicated blocks|"
                            r"Missing blocks|Corrupt blocks):\s*(\d+)", output, re.M):
        require(int(count) == 0, "fsck: недостаточная репликация или повреждённые блоки")


def parse_nodes(output, expected):
    total = re.findall(r"^Total Nodes:\s*(\d+)\s*$", output, re.M)
    rows = []
    for line in output.splitlines():
        fields = line.split()
        if fields and re.fullmatch(r"[^\s:]+:\d+", fields[0]):
            require(len(fields) == 4 and fields[3].isdigit(), "неизвестный формат NodeManager")
            rows.append(fields)
    require(total == [str(expected)] and len(rows) == expected,
            f"ожидалось {expected} NodeManager; total={total}, строк={len(rows)}")
    require(len({r[0] for r in rows}) == expected, "повторяющиеся NodeId")
    require(all(r[1] == "RUNNING" for r in rows), "не все NodeManager в RUNNING: " + str(rows))
    return rows


class QA:
    def __init__(self, args, config):
        self.args = args
        self.config = config
        self.failed = 0
        self.untested = 0
        self.optional_untested = 0

    def check(self, name, action):
        try:
            result = action()
        except OptionalNotTested as error:
            self.optional_untested += 1
            print(f"NOT TESTED (optional): {name}: {error}", flush=True)
        except NotTested as error:
            self.untested += 1
            print(f"NOT TESTED: {name}: {error}", flush=True)
        except (Failed, ValueError, KeyError, TypeError, AttributeError, OSError) as error:
            self.failed += 1
            print(f"FAIL: {name}: {error}", flush=True)
        else:
            print(f"PASS: {name}", flush=True)
            return result
        return None

    def command(self, argv):
        if not shutil.which(argv[0]):
            raise NotTested(f"не найден исполняемый файл {argv[0]}")
        env = dict(os.environ, LC_ALL="C", LANG="C")
        # Separate process group lets a timeout terminate the shell AND Java child.
        with subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              env=env, start_new_session=True) as process:
            try:
                out, err = process.communicate(timeout=self.args.timeout)
            except (subprocess.TimeoutExpired, KeyboardInterrupt):
                os.killpg(process.pid, signal.SIGKILL)
                process.communicate()
                if sys.exc_info()[0] is KeyboardInterrupt:
                    raise
                raise Failed(f"таймаут {self.args.timeout}s: {shlex.join(argv)}") from None
        if process.returncode:
            detail = (err + out).decode(errors="replace")[-4000:]
            raise Failed(f"exit={process.returncode}: {shlex.join(argv)}\n{detail}")
        return out

    def cli(self, tool, *args):
        binary = str(Path(os.environ["HADOOP_HOME"]) / "bin" / tool) if os.environ.get("HADOOP_HOME") else tool
        return self.command([binary, *args]).decode(errors="replace")

    def url(self, service):
        try:
            return self.config["services"][service]["url"].rstrip("/")
        except KeyError:
            raise NotTested(f"укажите services.{service}.url в --config") from None

    def http(self, service, path="/"):
        url = self.url(service) + path
        # No redirects, ambient proxies or ~/.curlrc; verify TLS, bounded request.
        raw = self.command(["curl", "--disable", "--silent", "--show-error",
                            "--noproxy", "*", "--connect-timeout", str(self.args.timeout),
                            "--max-time", str(self.args.timeout), "--proto", "=http,https",
                            "--header", "Accept: application/json" if path.startswith(("/jmx", "/ws/")) else "Accept: text/html",
                            "--write-out", "\n%{http_code}", url])
        body, code = raw.rsplit(b"\n", 1)
        require(code == b"200", f"{url}: HTTP {code.decode(errors='replace')} (нужен 200)")
        return body

    def api(self, service, path):
        return json.loads(self.http(service, path))

    def web(self, service, role=None):
        role = role or service
        path, marker = WEB_PAGES[role]
        page = Page()
        page.feed(self.http(service, path).decode("utf-8", errors="replace"))
        visible = " ".join(page.text)
        require(page.html and not page.password
                and not re.search(r"\b(?:log\s*in|sign\s*in)\b", visible, re.I)
                and re.search(marker, visible, re.I),
                f"{service}: HTTP 200, но страница не распознана как {role} UI или требует входа")
        # Unlike a generic 200 page, these endpoints identify the HDFS daemon.
        if role == "namenode":
            self.jmx(service, "Hadoop:service=NameNode,name=NameNodeInfo")
        elif role == "datanode":
            self.jmx(service, "Hadoop:service=DataNode,name=DataNodeInfo")

    def jmx(self, service, name):
        beans = self.api(service, "/jmx?qry=" + name)["beans"]
        require(isinstance(beans, list) and len(beans) == 1 and beans[0]["name"] == name,
                "нет ожидаемого JMX bean " + name)
        return beans[0]

    def group(self, group):
        if group not in self.config:
            raise NotTested(f"укажите список {group} в --config")
        return self.config[group]

    def secondary(self):
        return self.jmx("secondarynamenode", "Hadoop:service=SecondaryNameNode,name=SecondaryNameNodeInfo")

    def checkpoint(self, info):
        age = info.get("LastCheckpointDeltaMs")
        if age is None:
            raise NotTested("JMX не содержит LastCheckpointDeltaMs; возраст checkpoint неизвестен")
        if type(age) is int and age == -1:
            raise OptionalNotTested("первый checkpoint ещё не выполнен; это не ошибка доступности HDFS")
        require(type(age) is int and 0 <= age <= self.config.get("checkpoint_max_age_seconds", 7200) * 1000,
                f"устаревший/некорректный checkpoint: возраст {age} ms")

    def smoke(self):
        base = self.args.smoke_base
        token = uuid.uuid4().hex
        remote = base.rstrip("/") + "/qa-hdfs-" + token
        owned = False
        try:
            # No -p: atomic creation must succeed before anything can be removed.
            self.cli("hdfs", "dfs", "-mkdir", remote)
            owned = True
            self.cli("hdfs", "dfs", "-chmod", "700", remote)
            with tempfile.TemporaryDirectory(prefix="hdfs-qa-") as temp:
                source = Path(temp) / "payload"
                content = ("hdfs-qa " + token + "\n").encode()
                source.write_bytes(content)
                self.cli("hdfs", "dfs", "-put", str(source), remote + "/payload")
                actual = self.cli("hdfs", "dfs", "-cat", remote + "/payload").encode()
                require(actual == content, "прочитанные данные отличаются от записанных")
        finally:
            if owned:
                # Never recursive. Only the fixed file under our successfully-created UUID directory.
                try:
                    self.cli("hdfs", "dfs", "-rm", "-f", "-skipTrash", remote + "/payload")
                    self.cli("hdfs", "dfs", "-rmdir", remote)
                except (Failed, NotTested, OSError) as error:
                    raise Failed(f"cleanup: проверьте оставшийся путь {remote}: {error}") from error
            else:
                print(f"NOT TESTED: cleanup: владение {remote} не подтверждено; ничего не удалено", flush=True)

    def hdfs(self):
        self.check("HDFS CLI", lambda: require("Hadoop 3." in self.cli("hdfs", "version"), "неизвестная версия Hadoop"))
        fs = self.check("fs.defaultFS", lambda: self.hdfs_uri())
        if fs is None:
            self.untested += 1
            print("NOT TESTED: HDFS RPC/fsck/smoke: не подтверждён hdfs:// в клиентской конфигурации")
        else:
            self.check("NameNode / dfsadmin report / 3 Normal DataNode", lambda: parse_report(self.cli("hdfs", "dfsadmin", "-report")))
            self.check("NameNode safe mode OFF", lambda: require(
                re.search(r"^Safe mode is OFF\s*$", self.cli("hdfs", "dfsadmin", "-safemode", "get"), re.M), "safe mode не OFF"))
            self.check("fsck /", lambda: parse_fsck(self.cli("hdfs", "fsck", "/")))
        for service in ("namenode", "secondarynamenode"):
            self.check(service + " Web UI", lambda s=service: self.web(s))
        secondary = self.check("SecondaryNameNode JMX", self.secondary)
        if secondary is not None:
            self.check("SecondaryNameNode checkpoint", lambda: self.checkpoint(secondary))
        nodes = self.check("список DataNode UI", lambda: self.group("datanodes"))
        if nodes:
            self.check("число DataNode UI", lambda: require(len(nodes) == 3, "нужно 3 DataNode UI"))
            for service in nodes:
                self.check(service + " Web UI", lambda s=service: self.web(s, "datanode"))
        if self.args.smoke and fs:
            if self.failed or self.untested:
                self.untested += 1
                print("NOT TESTED: smoke: сначала устраните FAIL/NOT TESTED диагностики")
            else:
                self.check("HDFS запись/чтение/очистка", self.smoke)
        elif not self.args.smoke:
            self.optional_untested += 1
            print("NOT TESTED (optional): запись/чтение отключены; нужен --smoke и разрешение владельца")

    def hdfs_uri(self):
        uri = self.cli("hdfs", "getconf", "-confKey", "fs.defaultFS").strip()
        require(uri.startswith("hdfs://") and bool(urlsplit(uri).hostname),
                "fs.defaultFS не hdfs://; проверьте HADOOP_CONF_DIR")
        return uri

    def yarn(self):
        self.check("YARN CLI", lambda: require("Hadoop 3." in self.cli("yarn", "version"), "неизвестная версия Hadoop"))
        nodes = self.check("ожидаемые NodeManager из конфигурации", lambda: self.group("nodemanagers"))
        listing = self.check("ResourceManager RPC / yarn node -list -all", lambda: self.cli("yarn", "node", "-list", "-all"))
        if nodes and listing is not None:
            self.check("число, состояния и адреса NodeManager", lambda: self.yarn_nodes(listing, nodes))
        self.check("ResourceManager Web UI", lambda: self.web("resourcemanager"))
        self.check("ResourceManager REST STARTED", self.rm_info)
        if nodes:
            self.check("ResourceManager REST nodes", lambda: self.rm_nodes(nodes))
            for service in nodes:
                self.check(service + " Web UI", lambda s=service: self.web(s, "nodemanager"))
                self.check(service + " health", lambda s=service: self.nm_info(s))
        if "jobhistory_required" not in self.config:
            self.untested += 1
            print("NOT TESTED: задайте jobhistory_required явно по согласованию с владельцем YARN")
        elif self.config["jobhistory_required"]:
            self.check("JobHistory Web UI", lambda: self.web("jobhistory"))
            self.check("JobHistory REST", self.jhs_info)
        else:
            self.optional_untested += 1
            print("NOT TESTED (optional): JobHistory отключён в QA-конфигурации")

    def rm_info(self):
        info = self.api("resourcemanager", "/ws/v1/cluster/info")["clusterInfo"]
        require(info["state"] == "STARTED", "ResourceManager не STARTED")
        require(info.get("haState") in (None, "ACTIVE"), "ResourceManager не ACTIVE")

    def yarn_nodes(self, listing, services):
        rows = parse_nodes(listing, len(services))
        expected = {urlsplit(self.url(s)).netloc for s in services}
        require({r[2] for r in rows} == expected, "CLI: NodeManager HTTP адреса отличаются от QA-конфигурации")

    def rm_nodes(self, services):
        data = self.api("resourcemanager", "/ws/v1/cluster/nodes").get("nodes") or {}
        nodes = data.get("node", [])
        require(len(nodes) == len(services) and all(n["state"] == "RUNNING" for n in nodes),
                "REST: число/состояния NodeManager отличаются от ожидаемых")
        expected = {urlsplit(self.url(s)).netloc for s in services}
        actual = {n["nodeHTTPAddress"] for n in nodes}
        require(actual == expected, f"REST NodeManager адреса {actual}; в QA ожидаются {expected}")

    def nm_info(self, service):
        info = self.api(service, "/ws/v1/node/info")["nodeInfo"]
        require(info["nodeHealthy"] is True, "NodeManager сообщает unhealthy: " + str(info.get("healthReport")))

    def jhs_info(self):
        info = self.api("jobhistory", "/ws/v1/history/info")["historyInfo"]
        require(bool(info["hadoopVersion"]) and int(info["startedOn"]) > 0, "неполный ответ JobHistory")

    def result(self):
        code = 1 if self.failed else (2 if self.untested else 0)
        print(f"RESULT: {'FAIL' if code == 1 else 'NOT TESTED' if code == 2 else 'PASS'}; "
              f"failed={self.failed}; not_tested={self.untested}; "
              f"optional_not_tested={self.optional_untested}; exit={code}")
        return code


def tunnel(args, config):
    if not args.edge or not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]*@[A-Za-z0-9][A-Za-z0-9_.-]*", args.edge):
        raise ValueError("--edge: укажите user@host или user@SSH-config-alias")
    port(args.ssh_port)
    if not args.service or len(set(args.service)) != len(args.service):
        raise ValueError("укажите неповторяющиеся --service NAME")
    argv = ["ssh", "-N", "-T", "-a", "-x", "-o", "ExitOnForwardFailure=yes",
            "-o", "StrictHostKeyChecking=yes", "-o", "ServerAliveInterval=30",
            "-o", "ServerAliveCountMax=3", "-o", "ConnectTimeout=10",
            "-o", "ControlMaster=no", "-o", "ControlPath=none", "-p", str(args.ssh_port)]
    if args.identity_file:
        identity = Path(args.identity_file).expanduser().resolve()
        if not identity.is_file() or not os.access(identity, os.R_OK):
            raise ValueError("--identity-file: нужен существующий доступный для чтения файл")
        argv += ["-i", str(identity), "-o", "IdentitiesOnly=yes"]
    ports = set()
    for name in args.service:
        service = config.get("services", {}).get(name)
        if not service or "local_port" not in service:
            raise ValueError(f"{name}: нужны url и local_port в --config")
        url = endpoint(service["url"])
        local = service["local_port"]
        if local in ports:
            raise ValueError(f"повтор локального порта {local}")
        ports.add(local)
        argv += ["-L", f"127.0.0.1:{local}:{url.hostname}:{url.port}"]
        print(f"{name}: {url.scheme}://127.0.0.1:{local} -> {service['url']}", flush=True)
    argv.append(args.edge)
    print(shlex.join(argv), flush=True)
    if args.dry_run:
        print("NOT TESTED: dry-run, SSH не запускался")
        return 0
    if not shutil.which("ssh"):
        raise NotTested("не найден ssh")
    print("SSH работает на переднем плане; Ctrl-C закроет туннели. Целевые UI проверьте браузером.", flush=True)
    return subprocess.call(argv)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    subs = parser.add_subparsers(dest="mode", required=True)
    for mode in ("hdfs", "yarn", "tunnel"):
        sub = subs.add_parser(mode)
        sub.add_argument("--config", help="QA JSON; не Ansible vars и не Hadoop XML")
        if mode == "tunnel":
            sub.add_argument("--edge", required=True)
            sub.add_argument("--ssh-port", type=int, default=22)
            sub.add_argument("--identity-file", help="путь SSH-ключа, включая нестандартные имена и пробелы")
            sub.add_argument("--service", action="append", help="ключ из services; можно повторять")
            sub.add_argument("--dry-run", action="store_true")
        else:
            sub.add_argument("--timeout", type=positive, default=60, help="таймаут каждой команды, секунд (по умолчанию 60)")
        if mode == "hdfs":
            sub.add_argument("--smoke", action="store_true", help="разрешить создание и удаление собственных тестовых данных")
            sub.add_argument("--smoke-base", help="существующий личный каталог HDFS (обязателен для --smoke)")
    args = parser.parse_args(argv)
    try:
        config = load_config(args.config)
        if args.mode == "tunnel":
            return tunnel(args, config)
        if args.mode == "hdfs":
            if args.smoke and (not args.smoke_base or not re.fullmatch(r"/(?:[A-Za-z0-9_.-]+/)*[A-Za-z0-9_.-]+", args.smoke_base)
                               or any(x in (".", "..") for x in args.smoke_base.split("/"))):
                raise ValueError("--smoke требует --smoke-base /существующий/личный-каталог без glob и '..'")
            if args.smoke_base and not args.smoke:
                raise ValueError("--smoke-base используется только с --smoke")
        qa = QA(args, config)
        getattr(qa, args.mode)()
        return qa.result()
    except (ValueError, OSError) as error:
        print(f"NOT TESTED: конфигурация/аргументы: {error}", file=sys.stderr)
        return 2
    except NotTested as error:
        print(f"NOT TESTED: {error}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("FAIL: выполнение прервано", file=sys.stderr)
        return 130


def interrupted(_signum, _frame):
    raise KeyboardInterrupt()


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, interrupted)
    sys.exit(main())
