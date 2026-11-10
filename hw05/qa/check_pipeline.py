#!/usr/bin/env python3
"""Read-only HW5 checks. No orchestration, spark-submit, DDL or data writes."""
import argparse
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import re
import signal
import sys
from urllib.parse import urlsplit
from uuid import UUID

ROOT = Path(__file__).resolve().parents[2]


def module(name, relative):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


transport = module("hw05_transport", "shared/qa/check_cluster.py")
hive = module("hw05_hive", "hw04-spark/qa/hive_integration.py")
Failed, NotTested, require = transport.Failed, transport.NotTested, transport.require


def uuid(value):
    require(isinstance(value, str) and str(UUID(value)) == value, "нужен UUID в каноническом виде")
    return value


def timestamp(value):
    require(isinstance(value, str), "нет времени запуска/завершения")
    date = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(date.tzinfo is not None, "время должно содержать timezone")
    return int(date.timestamp() * 1000)


def exact_keys(value, keys):
    require(isinstance(value, dict) and set(value) == set(keys),
            "ожидаются только поля: " + ", ".join(sorted(keys)))


def hdfs_uri(value):
    hive.clean_location(value)
    url = urlsplit(value)
    require(url.scheme == "hdfs" and url.hostname and url.port
            and re.fullmatch(r"[A-Za-z0-9_.-]+", url.hostname)
            and 1 <= url.port <= 65535, "нужен полный hdfs://host:port/path")
    return value.rstrip("/")


def config(path):
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    exact_keys(data, {"prefect", "yarn", "input", "hive", "clock_skew_seconds"})
    exact_keys(data["prefect"], {"api_url", "flow_id"})
    api = data["prefect"]["api_url"]
    require(isinstance(api, str) and api.endswith("/api"), "prefect.api_url должен оканчиваться /api")
    transport.endpoint(api[:-4])
    if data["prefect"]["flow_id"] is not None:
        uuid(data["prefect"]["flow_id"])
    exact_keys(data["yarn"], {"url", "user"})
    transport.endpoint(data["yarn"]["url"])
    require(isinstance(data["yarn"]["user"], str) and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]*", data["yarn"]["user"]), "некорректный YARN user")
    data["input"] = hdfs_uri(data["input"])
    table = data["hive"]
    exact_keys(table, {"jdbc_url", "user", "database", "table", "location", "columns",
                       "partition_column", "expect_partitions", "min_rows", "expected_rows"})
    require(isinstance(table["jdbc_url"], str) and re.fullmatch(
        r"jdbc:hive2://[A-Za-z0-9_.-]+:[0-9]{1,5}/[A-Za-z_][A-Za-z0-9_]*", table["jdbc_url"]), "некорректный JDBC URL; credentials в JSON запрещены")
    port = int(table["jdbc_url"].split(":")[-1].split("/")[0])
    require(1 <= port <= 65535, "некорректный JDBC port")
    require(isinstance(table["user"], str) and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]*", table["user"]), "некорректный Hive user")
    hive.ident(table["database"])
    hive.ident(table["table"])
    table["location"] = hdfs_uri(table["location"])
    require(urlsplit(table["location"]).netloc == urlsplit(data["input"]).netloc,
            "input и result должны находиться в одном HDFS")
    require(isinstance(table["columns"], list) and table["columns"], "нужны columns")
    columns = [hive.column(col) for col in table["columns"]]
    part = table["partition_column"]
    if part is not None:
        part_name, _ = hive.column(part)
        columns.append(hive.column(part))
        require(isinstance(table["expect_partitions"], list) and table["expect_partitions"], "нужны expect_partitions")
        for value in table["expect_partitions"]:
            require(isinstance(value, str) and re.fullmatch(re.escape(part_name) + r"=[A-Za-z0-9_-]+", value), "некорректная партиция")
        require(len(set(table["expect_partitions"])) == len(table["expect_partitions"]), "повторяющиеся партиции")
    else:
        require(table["expect_partitions"] == [], "без partition_column expect_partitions должен быть []")
    names = [name.lower() for name, _ in columns]
    require(len(set(names)) == len(names), "повторяющиеся колонки")
    require(type(table["min_rows"]) is int and table["min_rows"] > 0, "min_rows должен быть > 0")
    expected = table["expected_rows"]
    require(expected is None or (isinstance(expected, list) and expected
            and all(isinstance(row, list) and len(row) == len(columns)
                    and all(isinstance(cell, str) and not any(x in cell for x in "\t\r\n") for cell in row)
                    for row in expected)), "expected_rows: null или непустой массив строковых ячеек в порядке columns, partition_column")
    require(type(data["clock_skew_seconds"]) is int and 0 <= data["clock_skew_seconds"] <= 60,
            "clock_skew_seconds: 0..60")
    return data


class Checks(transport.QA):
    def __init__(self, args, data):
        super().__init__(args, {})
        self.data = data
        self.results = []

    def check(self, name, action):
        try:
            value = action()
        except NotTested as error:
            status, detail, value = "NOT TESTED", str(error), None
        except (Failed, hive.QaError, ValueError, KeyError, TypeError, AttributeError, OSError) as error:
            status, detail, value = "FAIL", str(error), None
        else:
            status, detail = "PASS", str(value)
        self.results.append({"check": name, "status": status, "detail": detail})
        print(f"{status}: {name}: {detail}", flush=True)
        return value

    def json(self, url):
        # Reuse bounded curl transport: no redirects, proxies, .curlrc or disabled TLS.
        parsed = urlsplit(url)
        self.config = {"services": {"request": {"url": f"{parsed.scheme}://{parsed.netloc}"}}}
        return json.loads(self.http("request", parsed.path))

    def hdfs_command(self, *args):
        return self.command([self.args.hdfs_bin, "dfs", *args]).decode(errors="replace")

    def input_data(self):
        uri = self.data["input"]
        output = self.hdfs_command("-count", uri).strip().split()
        require(len(output) == 4 and all(x.isdigit() for x in output[:3]), "неизвестный формат hdfs dfs -count")
        require(int(output[1]) > 0 and int(output[2]) > 0, "вход HDFS пуст")
        return f"{uri}: files={output[1]}, bytes={output[2]}"

    def flow(self):
        expected = self.data["prefect"]["flow_id"]
        if expected is None or not self.args.flow_run_id:
            raise NotTested("задайте prefect.flow_id и --flow-run-id реального HW5 запуска")
        run = self.json(self.data["prefect"]["api_url"] + "/flow_runs/" + self.args.flow_run_id)
        require(run["id"] == self.args.flow_run_id and run["flow_id"] == expected, "Flow не соответствует контракту HW5")
        require(run["state_type"] == "COMPLETED" and run["state"]["type"] == "COMPLETED",
                f"Flow не COMPLETED: state_type={run['state_type']}, state={run['state']['type']}")
        start, end = timestamp(run["start_time"]), timestamp(run["end_time"])
        require(0 < start < end, "некорректный интервал Flow")
        return {"id": run["id"], "start": start, "end": end}

    def rm(self):
        info = self.json(self.data["yarn"]["url"].rstrip("/") + "/ws/v1/cluster/info")["clusterInfo"]
        require(info["state"] == "STARTED" and info.get("haState") in (None, "ACTIVE"), "RM не STARTED/ACTIVE")
        return "ResourceManager STARTED"

    def application(self, flow):
        if flow is None or not self.args.application_id:
            raise NotTested("нужны проверенный Flow и --application-id из его spark-submit")
        app = self.json(self.data["yarn"]["url"].rstrip("/") + "/ws/v1/cluster/apps/" + self.args.application_id)["app"]
        require(app["id"] == self.args.application_id and app["applicationType"].upper() == "SPARK", "не то Spark-приложение")
        require(app["state"] == "FINISHED" and app["finalStatus"] == "SUCCEEDED",
                f"YARN не FINISHED/SUCCEEDED: state={app['state']}, finalStatus={app['finalStatus']}")
        require(app["user"] == self.data["yarn"]["user"], "не тот пользователь YARN")
        tags = {tag.strip() for tag in app["applicationTags"].split(",")}
        require("prefect-flow-run-id=" + flow["id"] in tags, "нет YARN tag, связывающего приложение с Flow")
        start, end = app["startedTime"], app["finishedTime"]
        skew = self.data["clock_skew_seconds"] * 1000
        require(type(start) is int and type(end) is int and 0 < start < end
                and flow["start"] - skew <= start < end <= flow["end"] + skew,
                "Spark не выполнялся внутри интервала Flow")
        return {"id": app["id"], "start": start, "end": end}

    def sql(self, sql):
        table = self.data["hive"]
        text = self.command([self.args.beeline_bin, "-u", table["jdbc_url"], "-n", table["user"],
                             "--force=false", "--silent=true", "--showHeader=false",
                             "--outputformat=tsv2", "-e", sql]).decode(errors="replace")
        require(not re.search(r"(?im)^\s*(?:Error:|FAILED:|Exception\b)", text), "Beeline сообщил ошибку SQL")
        return text

    def table(self):
        table = self.data["hive"]
        full = hive.ident(table["database"]) + "." + hive.ident(table["table"])
        columns = [hive.column(col) for col in table["columns"]]
        if table["partition_column"]:
            columns.append(hive.column(table["partition_column"]))
        # DESCRIBE repeats partition columns in the second section; inspect first section only.
        description = self.sql("DESCRIBE " + full).split("# Partition Information")[0]
        actual = []
        for line in description.splitlines():
            cells = [cell.strip() for cell in line.split("\t")]
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            require(len(cells) >= 2, "неизвестный формат DESCRIBE tsv2")
            actual.append((cells[0].lower(), cells[1].upper()))
        require(actual == [(n.lower(), t) for n, t in columns], f"схема отличается: {actual}")
        formatted = self.sql("DESCRIBE FORMATTED " + full)
        locations = re.findall(r"(?m)^\s*Location:\s*\t\s*(\S+)", formatted)
        require(locations == [table["location"]],
                f"Hive LOCATION {locations} отличается от ожидаемого {table['location']}")
        count_text = self.sql("SELECT COUNT(*) FROM " + full).strip()
        require(re.fullmatch(r"\d+", count_text), "COUNT(*) не вернул ровно одно целое число")
        count = hive.count_result(count_text)
        require(count >= table["min_rows"], f"всего {count} строк, мало данных")
        if table["partition_column"]:
            parts = {line.strip() for line in self.sql("SHOW PARTITIONS " + full).splitlines() if line.strip()}
            require(parts == set(table["expect_partitions"]),
                    f"набор партиций {sorted(parts)} отличается от {table['expect_partitions']}")
            col = hive.ident(table["partition_column"]["name"])
            for part in sorted(parts):
                value = part.split("=", 1)[1]
                rows = self.sql(f"SELECT COUNT(*) FROM {full} WHERE {col} = '{value}'").strip()
                require(re.fullmatch(r"\d+", rows) and int(rows) > 0, "пустая партиция " + part)
        names = ", ".join(hive.ident(n) for n, _ in columns)
        # SELECT forces physical column reads even when COUNT uses metastore statistics.
        expected = table["expected_rows"]
        limit = len(expected) + 1 if expected else 10
        sample = self.sql(f"SELECT {names} FROM {full} LIMIT {limit}")
        rows = [line.split("\t") for line in sample.splitlines() if line.strip()]
        require(rows and all(len(row) == len(columns) for row in rows), "не удалось прочитать столбцы таблицы")
        if expected:
            require(count == len(expected) and sorted(rows) == sorted(expected), "данные не совпали с ожидаемым результатом преобразований")
        return {"table": full, "rows": count, "location": locations[0]}

    def freshness(self, flow, app, table):
        if flow is None or app is None or table is None:
            raise NotTested("нужны проверенные Flow, Spark/YARN и Hive-таблица")
        config = self.data["hive"]
        folders = config["expect_partitions"] if config["partition_column"] else [""]
        skew = self.data["clock_skew_seconds"] * 1000
        for folder in folders:
            path = table["location"] + ("/" + folder if folder else "") + "/part-*"
            values = self.hdfs_command("-stat", "%Y", path).splitlines()
            require(values and all(v.isdigit() for v in values), "нет mtime файлов результата")
            require(all(app["start"] - skew <= int(v) <= app["end"] + skew for v in values),
                    f"устаревшие/посторонние файлы {path}: mtime={values}, "
                    f"допустимо {app['start'] - skew}..{app['end'] + skew} ms UTC")
        return "все part-* ожидаемых партиций записаны во время связанного Spark-приложения"

    def run(self):
        self.check("Prefect API", lambda: require(self.json(self.data["prefect"]["api_url"] + "/health") is True,
                                                  "health не вернул JSON true") or "healthy")
        self.check("HDFS input", self.input_data)
        flow = self.check("Prefect Flow run", self.flow)
        self.check("YARN API", self.rm)
        app = self.check("Spark in YARN / correlation", lambda: self.application(flow))
        table = self.check("Hive table / schema / rows / partitions / read", self.table)
        self.check("Result freshness", lambda: self.freshness(flow, app, table))
        statuses = {item["status"] for item in self.results}
        status = "FAIL" if "FAIL" in statuses else "NOT TESTED" if "NOT TESTED" in statuses else "PASS"
        code = {"PASS": 0, "FAIL": 1, "NOT TESTED": 2}[status]
        report = {"status": status, "exit_code": code, "checked_at": datetime.now(timezone.utc).isoformat(),
                  "flow_run_id": self.args.flow_run_id, "application_id": self.args.application_id,
                  "checks": self.results}
        if self.args.report:
            # Exclusive creation: never overwrite a previous evidence file or follow a symlink.
            try:
                with Path(self.args.report).open("x", encoding="utf-8") as out:
                    json.dump(report, out, ensure_ascii=False, indent=2)
                    out.write("\n")
            except OSError as error:
                # A late report error must not mask a failed cluster check with exit 2.
                status, code = "FAIL", 1
                print(f"FAIL: запись отчёта: {error}", file=sys.stderr)
        print(f"RESULT: {status}; exit={code}")
        return code


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--flow-run-id")
    parser.add_argument("--application-id")
    parser.add_argument("--hdfs-bin", default="hdfs")
    parser.add_argument("--beeline-bin", default="beeline")
    parser.add_argument("--timeout", type=transport.positive, default=60, help="таймаут каждой HTTP/CLI операции, секунд")
    parser.add_argument("--report", help="новый JSON-файл доказательств; существующий файл не перезаписывается")
    args = parser.parse_args(argv)
    try:
        data = config(args.config)
        if args.flow_run_id:
            uuid(args.flow_run_id)
        if args.application_id:
            require(re.fullmatch(r"application_[0-9]+_[0-9]+", args.application_id), "некорректный application ID")
        if args.report:
            require(not Path(args.report).exists() and not Path(args.report).is_symlink(), "report уже существует")
        return Checks(args, data).run()
    except (Failed, hive.QaError, ValueError, TypeError, AttributeError, OSError) as error:
        print(f"NOT TESTED: конфигурация/отчёт: {error}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("NOT TESTED: выполнение прервано", file=sys.stderr)
        return 130


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, transport.interrupted)
    sys.exit(main())
