#!/usr/bin/env python3
"""Read-only Hive SQL and partition QA via Beeline."""
import argparse
import re
import shutil
import subprocess
import sys

IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
PART = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)=([A-Za-z0-9_-]+)\Z")

class Failure(Exception):
    pass

def validate_name(name):
    if not IDENT.fullmatch(name):
        raise ValueError(f"Недопустимое имя Hive: {name!r}")
    return f"`{name}`"

def validate_partition(part, column):
    match = PART.fullmatch(part)
    if not match or match.group(1) != column:
        raise ValueError(f"Ожидается {column}=значение, получено {part!r}")
    return match.group(2)

def numeric_result(output):
    rows = [line.strip() for line in output.splitlines() if re.fullmatch(r"[0-9]+", line.strip())]
    if len(rows) != 1:
        raise Failure("COUNT(*) не вернул ровно одну числовую строку (tsv2)")
    return int(rows[0])

def partition_result(output, column):
    rows = set()
    for line in output.splitlines():
        value = line.strip()
        if value.startswith(column + "="):
            validate_partition(value, column)
            rows.add(value)
    if not rows:
        raise Failure(f"SHOW PARTITIONS не вернул партиций по {column}")
    return rows

def query(args, sql):
    cmd = [args.beeline_bin, "-u", args.jdbc_url, "-n", args.user,
           "--silent=true", "--showHeader=false", "--outputformat=tsv2", "-e", sql]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, errors="replace",
                              timeout=args.timeout, check=False)
    except subprocess.TimeoutExpired as exc:
        raise Failure(f"Beeline: превышен таймаут {args.timeout} с") from exc
    except OSError as exc:
        raise Failure(f"Beeline не запустился: {exc}") from exc
    if proc.returncode != 0 or re.search(r"(?im)^\s*(?:Error:|FAILED:|Exception\b)", proc.stdout):
        raise Failure(f"Ошибка SQL (exit {proc.returncode}): {(proc.stderr.strip() or proc.stdout.strip())[-700:]}")
    return proc.stdout

def main(argv=None):
    p = argparse.ArgumentParser(description="HiveServer2: SQL, строки и партиции (только чтение)")
    p.add_argument("--jdbc-url", required=True)
    p.add_argument("--user", default="team")
    p.add_argument("--database", default="default")
    p.add_argument("--table", default="books")
    p.add_argument("--partition-column", default="year")
    p.add_argument("--min-rows", type=int, default=1)
    p.add_argument("--expect-partition", action="append", default=[])
    p.add_argument("--beeline-bin", default="beeline")
    p.add_argument("--timeout", type=int, default=45)
    args = p.parse_args(argv)
    if not args.jdbc_url.startswith("jdbc:hive2://") or not args.jdbc_url[len("jdbc:hive2://"):]:
        p.error("--jdbc-url: требуется jdbc:hive2://host:port/database")
    if args.min_rows < 1 or args.timeout < 1:
        p.error("--min-rows и --timeout должны быть положительными")
    try:
        db, table, col = (validate_name(v) for v in (args.database, args.table, args.partition_column))
        for part in args.expect_partition:
            validate_partition(part, args.partition_column)
    except ValueError as exc:
        p.error(str(exc))
    if shutil.which(args.beeline_bin) is None:
        print("NOT TESTED: beeline не найден в PATH (или --beeline-bin)")
        return 2
    try:
        name = f"{db}.{table}"
        n = numeric_result(query(args, f"SELECT COUNT(*) FROM {name}"))
        if n < args.min_rows:
            raise Failure(f"Найдено {n} строк, требуется минимум {args.min_rows}")
        print(f"PASS: HiveServer2 и SQL: {name}, строк {n}")
        partitions = partition_result(query(args, f"SHOW PARTITIONS {name}"), args.partition_column)
        missing = set(args.expect_partition) - partitions
        if missing:
            raise Failure(f"Нет партиций: {', '.join(sorted(missing))}")
        print(f"PASS: партиции: {', '.join(sorted(partitions))}")
        for part in args.expect_partition:
            val = validate_partition(part, args.partition_column)
            count = numeric_result(query(args, f"SELECT COUNT(*) FROM {name} WHERE {col} = '{val}'"))
            if count < 1:
                raise Failure(f"Партиция {part} пуста")
            print(f"PASS: партиция {part}, строк {count}")
    except (Failure, ValueError) as exc:
        print(f"FAIL: {exc}")
        return 1
    print("RESULT: PASS; exit=0")
    return 0

if __name__ == '__main__':
    sys.exit(main())
