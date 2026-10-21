#!/usr/bin/env python3
"""Register and verify Spark-produced partitioned Parquet using HiveServer2."""
import argparse
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlsplit

IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
SUPPORTED_TYPES = {"STRING", "INT", "BIGINT", "DOUBLE", "FLOAT", "BOOLEAN", "DATE", "TIMESTAMP", "SMALLINT", "TINYINT"}
PARTITION_VALUE = re.compile(r"[A-Za-z0-9_-]+\Z")
SAFE_PATH = re.compile(r"[A-Za-z0-9_./:=@-]+\Z")


class QaError(Exception):
    pass


def ident(value):
    if not isinstance(value, str) or not IDENT.fullmatch(value):
        raise QaError(f"Некорректный SQL-идентификатор: {value!r}")
    return f"`{value}`"


def column(value):
    if not isinstance(value, dict) or set(value) != {"name", "type"}:
        raise QaError("Колонка должна иметь поля name и type")
    name = value["name"]
    ident(name)
    typ = value["type"]
    if not isinstance(typ, str) or typ.upper() not in SUPPORTED_TYPES:
        raise QaError(f"Неподдерживаемый тип столбца: {typ!r}")
    return name, typ.upper()


def clean_location(location):
    if not isinstance(location, str) or not SAFE_PATH.fullmatch(location):
        raise QaError("LOCATION содержит запрещённые символы")
    u = urlsplit(location)
    if u.scheme not in ('', 'hdfs') or u.query or u.fragment or (u.scheme and not location.startswith('hdfs://')):
        raise QaError("LOCATION должен быть абсолютным путём в HDFS или hdfs:// URI")
    path = u.path
    if not path.startswith('/') or path == '/' or any(part in ('.', '..') for part in path.split('/')):
        raise QaError("LOCATION должен быть безопасным абсолютным путём не равным корню")
    if u.scheme == '' and not location.startswith('/'):
        raise QaError("LOCATION должен быть абсолютным")
    if u.scheme == 'hdfs' and ('@' in u.netloc or '\\' in u.netloc):
        raise QaError("Некорректный HDFS URI")
    return location.rstrip('/')


def read_config(path):
    try:
        data = json.loads(Path(path).read_text(encoding='utf-8'))
    except (OSError, ValueError) as exc:
        raise QaError(f"Не удалось прочитать JSON конфигурацию: {exc}") from exc
    required = {"jdbc_url", "user", "database", "table", "location", "columns", "partition_column", "expect_partitions", "min_rows"}
    if not isinstance(data, dict) or not required.issubset(data):
        raise QaError("В JSON отсутствуют обязательные поля")
    if not isinstance(data['jdbc_url'], str) or not re.fullmatch(r'jdbc:hive2://[A-Za-z0-9_.-]+:[0-9]{1,5}/[A-Za-z_][A-Za-z0-9_]*', data['jdbc_url']):
        raise QaError("jdbc_url: ожидается jdbc:hive2://host:port/database")
    jdbc_port = int(data['jdbc_url'].split(':')[-1].split('/')[0])
    if not 1 <= jdbc_port <= 65535:
        raise QaError("Некорректный порт JDBC")
    if not isinstance(data['user'], str) or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_.-]*', data['user']):
        raise QaError("Некорректный Hive user")
    db, table = ident(data['database']), ident(data['table'])
    loc = clean_location(data['location'])
    if not isinstance(data['columns'], list) or not data['columns']:
        raise QaError("Требуется непустой массив columns")
    columns = [column(c) for c in data['columns']]
    part_name, part_type = column(data['partition_column'])
    names = [n.lower() for n, _ in columns] + [part_name.lower()]
    if len(names) != len(set(names)):
        raise QaError("Имена столбцов должны быть уникальными")
    if not isinstance(data['expect_partitions'], list) or not data['expect_partitions']:
        raise QaError("Укажите хотя бы одну ожидаемую партицию")
    for part in data['expect_partitions']:
        if not isinstance(part, str) or not part.startswith(part_name + '=') or not PARTITION_VALUE.fullmatch(part[len(part_name)+1:]):
            raise QaError(f"Некорректная партиция: {part!r}")
    if type(data['min_rows']) is not int or data['min_rows'] < 1:
        raise QaError("min_rows должно быть положительным целым числом")
    data['_full_name'] = f"{db}.{table}"
    data['_location'] = loc
    data['_columns'] = columns
    data['_part_name'] = part_name
    data['_part_type'] = part_type
    return data


def statements(data):
    cols = ',\n  '.join(f'{ident(n)} {t}' for n, t in data['_columns'])
    return [
        f"CREATE EXTERNAL TABLE IF NOT EXISTS {data['_full_name']} (\n  {cols}\n)\n"
        f"PARTITIONED BY ({ident(data['_part_name'])} {data['_part_type']})\n"
        f"STORED AS PARQUET\nLOCATION '{data['_location']}'",
        f"MSCK REPAIR TABLE {data['_full_name']}",
    ]


def run_beeline(data, sql, binary, timeout):
    cmd = [binary, '-u', data['jdbc_url'], '-n', data['user'],
           '--silent=true', '--showHeader=false', '--outputformat=tsv2', '-e', sql]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, errors='replace', timeout=timeout)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise QaError(f"Не удалось выполнить Beeline: {exc}") from exc
    if proc.returncode != 0 or re.search(r'(?im)^\s*(?:Error:|FAILED:|Exception\b)', proc.stdout):
        raise QaError(f"Ошибка Beeline, exit={proc.returncode}: {(proc.stderr.strip() or proc.stdout.strip())[-800:]}")
    return proc.stdout


def count_result(output):
    rows = [line.strip() for line in output.splitlines() if re.fullmatch(r'[0-9]+', line.strip())]
    if len(rows) != 1:
        raise QaError('COUNT(*) не вернул одну числовую строку')
    return int(rows[0])


def verify(data, binary, timeout):
    full = data['_full_name']
    count = count_result(run_beeline(data, f'SELECT COUNT(*) FROM {full}', binary, timeout))
    if count < data['min_rows']:
        raise QaError(f'Недостаточно данных: {count} строк, требуется {data["min_rows"]}')
    print(f'PASS: Hive SQL, таблица {full}, строк {count}')
    parts_output = run_beeline(data, f'SHOW PARTITIONS {full}', binary, timeout)
    parts = {line.strip() for line in parts_output.splitlines() if line.strip().startswith(data['_part_name']+'=')}
    missing = set(data['expect_partitions']) - parts
    if missing:
        raise QaError(f'Отсутствуют партиции: {", ".join(sorted(missing))}')
    print(f'PASS: Hive partitions: {", ".join(sorted(set(data["expect_partitions"])))}')
    sample_col = ident(data['_columns'][0][0])
    for part in data['expect_partitions']:
        value = part.split('=', 1)[1]
        n = count_result(run_beeline(data, f"SELECT COUNT(*) FROM {full} WHERE {ident(data['_part_name'])} = '{value}'", binary, timeout))
        if n < 1:
            raise QaError(f'Партиция {part} пуста')
        print(f'PASS: {part}: {n} строк')
    # Forces Hive to read a physical Parquet data column, not only partition metadata.
    sample = run_beeline(data, f'SELECT {sample_col} FROM {full} LIMIT 1', binary, timeout)
    if not any(line.strip() and not line.strip().startswith(('WARN', 'INFO', 'SLF4J')) for line in sample.splitlines()):
        raise QaError('Hive не вернул ни одной строки Parquet-данных')
    print('PASS: Hive прочитал столбец из Parquet')


def main(argv=None):
    parser = argparse.ArgumentParser(description='Spark Parquet → Hive external table and read QA')
    parser.add_argument('action', choices=['ddl', 'apply', 'verify'])
    parser.add_argument('--config', required=True)
    parser.add_argument('--beeline-bin', default='beeline')
    parser.add_argument('--timeout', type=int, default=90)
    args = parser.parse_args(argv)
    if args.timeout < 1:
        parser.error('--timeout должен быть положительным')
    try:
        data = read_config(args.config)
        if args.action == 'ddl':
            print(';\n\n'.join(statements(data)) + ';')
            print('\nNOT TESTED: SQL только показан, Hive не изменён')
            return 0
        if shutil.which(args.beeline_bin) is None:
            print('NOT TESTED: Beeline не найден, задайте --beeline-bin')
            return 2
        if args.action == 'apply':
            for sql in statements(data):
                run_beeline(data, sql, args.beeline_bin, args.timeout)
            print('PASS: таблица создана (или уже существовала), партиции зарегистрированы')
        verify(data, args.beeline_bin, args.timeout)
    except QaError as exc:
        print(f'FAIL: {exc}')
        return 1
    print('RESULT: PASS; exit=0')
    return 0


if __name__ == '__main__':
    sys.exit(main())
