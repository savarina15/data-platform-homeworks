import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock
import importlib.util

SOURCE = Path(__file__).resolve().parents[1] / 'hive_integration.py'
spec = importlib.util.spec_from_file_location('hive_integration', SOURCE)
hi = importlib.util.module_from_spec(spec)
spec.loader.exec_module(hi)
EXAMPLE = json.loads((Path(__file__).resolve().parents[1] / 'spark_hive.example.json').read_text())


class IntegrationTests(unittest.TestCase):
    def load(self, changes=None):
        data = json.loads(json.dumps(EXAMPLE))
        if changes:
            data.update(changes)
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'config.json'
            p.write_text(json.dumps(data))
            return hi.read_config(p)

    def test_good_config(self):
        data = self.load()
        self.assertEqual(data['_full_name'], '`default`.`books_summary_spark`')

    def test_ddl_parquet_external_and_repair(self):
        sql = hi.statements(self.load())
        self.assertIn('CREATE EXTERNAL TABLE', sql[0])
        self.assertIn('PARTITIONED BY (`year` INT)', sql[0])
        self.assertIn('STORED AS PARQUET', sql[0])
        self.assertIn("LOCATION '/user/team/spark/books_summary'", sql[0])
        self.assertIn('MSCK REPAIR TABLE', sql[1])

    def test_reject_sql_name_injection(self):
        with self.assertRaises(hi.QaError):
            self.load({'table': 'books; DROP TABLE students'})

    def test_reject_path_injection(self):
        with self.assertRaises(hi.QaError):
            self.load({'location': "/x'; DROP TABLE x; --"})

    def test_reject_relative_location(self):
        with self.assertRaises(hi.QaError):
            self.load({'location': 'relative/folder'})

    def test_reject_parent_reference(self):
        with self.assertRaises(hi.QaError):
            self.load({'location': '/user/team/../hive'})

    def test_reject_duplicate_column(self):
        with self.assertRaises(hi.QaError):
            self.load({'columns': [{'name': 'year', 'type': 'STRING'}]})

    def test_reject_partition_injection(self):
        with self.assertRaises(hi.QaError):
            self.load({'expect_partitions': ["year=2023' OR 1=1"]})

    def test_count_parse(self):
        self.assertEqual(hi.count_result('INFO test\n3\n'), 3)
        with self.assertRaises(hi.QaError):
            hi.count_result('unknown')

    def test_verify_pass(self):
        d = self.load()
        def runner(data, sql, binary, timeout):
            if 'COUNT(*)' in sql:
                return '2\n'
            if 'SHOW PARTITIONS' in sql:
                return 'year=2022\nyear=2023\n'
            if 'LIMIT 1' in sql:
                return 'Fiction\n'
            raise AssertionError(sql)
        with mock.patch.object(hi, 'run_beeline', side_effect=runner):
            hi.verify(d, 'beeline', 30)

    def test_verify_missing_partition(self):
        d = self.load()
        with mock.patch.object(hi, 'run_beeline', side_effect=['2\n', 'year=2022\n']):
            with self.assertRaisesRegex(hi.QaError, 'Отсутствуют партиции'):
                hi.verify(d, 'beeline', 30)

    def test_verify_empty_partition(self):
        d = self.load()
        with mock.patch.object(hi, 'run_beeline', side_effect=['2\n', 'year=2022\nyear=2023\n', '0\n']):
            with self.assertRaisesRegex(hi.QaError, 'пуста'):
                hi.verify(d, 'beeline', 30)

    def test_verify_no_sample(self):
        d = self.load()
        with mock.patch.object(hi, 'run_beeline', side_effect=['2\n', 'year=2022\nyear=2023\n', '1\n','1\n', '']):
            with self.assertRaisesRegex(hi.QaError, 'не вернул'):
                hi.verify(d, 'beeline', 30)

    def test_ddl_not_touching_hive(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'config.json';p.write_text(json.dumps(EXAMPLE))
            with mock.patch.object(hi, 'run_beeline') as run:
                self.assertEqual(hi.main(['ddl','--config',str(p)]),0)
                run.assert_not_called()

    def test_missing_beeline(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'config.json';p.write_text(json.dumps(EXAMPLE))
            with mock.patch.object(hi.shutil,'which', return_value=None):
                self.assertEqual(hi.main(['verify','--config',str(p)]),2)

    def test_apply_statements_first(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'config.json';p.write_text(json.dumps(EXAMPLE))
            calls=[]
            def fake_run(data,sql,binary,timeout):
                calls.append(sql)
                if 'COUNT(*)' in sql:return '1\n'
                if 'SHOW PARTITIONS' in sql:return 'year=2022\nyear=2023\n'
                if 'LIMIT 1' in sql:return 'genre\n'
                return ''
            with mock.patch.object(hi.shutil,'which', return_value='/bin/true'):
                with mock.patch.object(hi,'run_beeline', side_effect=fake_run):
                    self.assertEqual(hi.main(['apply','--config',str(p)]),0)
            self.assertIn('CREATE EXTERNAL TABLE',calls[0])
            self.assertIn('MSCK REPAIR TABLE',calls[1])


if __name__ == '__main__':
    unittest.main()
