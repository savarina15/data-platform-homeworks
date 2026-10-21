"""Offline mock tests for Hive SQL QA."""
import importlib.util
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

path = Path(__file__).resolve().parents[1] / 'check_hive.py'
spec = importlib.util.spec_from_file_location('check_hive', path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)

class HiveTests(unittest.TestCase):
    def test_count(self):
        self.assertEqual(mod.numeric_result('3\n'), 3)
    def test_not_count(self):
        with self.assertRaises(mod.Failure): mod.numeric_result('Login page\n')
    def test_multiple_rows(self):
        with self.assertRaises(mod.Failure): mod.numeric_result('3\n5\n')
    def test_valid_partitions(self):
        self.assertEqual(mod.partition_result('year=2022\nyear=2023\n', 'year'), {'year=2022','year=2023'})
    def test_missing_partitions(self):
        with self.assertRaises(mod.Failure): mod.partition_result('Empty\n', 'year')
    def test_sql_identifier_injection(self):
        with self.assertRaises(ValueError): mod.validate_name('books;DROP TABLE x')
    def test_sql_partition_injection(self):
        with self.assertRaises(ValueError): mod.validate_partition("year=2022'OR 1=1", 'year')
    def test_query_failure(self):
        p = subprocess.CompletedProcess([], 1, '', 'Connection refused')
        with patch.object(mod.subprocess, 'run', return_value=p):
            with self.assertRaises(mod.Failure): mod.query(self.args(), 'SELECT 1')
    def test_timeout(self):
        with patch.object(mod.subprocess, 'run', side_effect=subprocess.TimeoutExpired('beeline', 5)):
            with self.assertRaises(mod.Failure): mod.query(self.args(), 'SELECT 1')
    @staticmethod
    def args():
        class Args:
            beeline_bin='beeline'; jdbc_url='jdbc:hive2://localhost:10000/default'; user='team'; timeout=5
        return Args()
    def test_full_pass(self):
        def command(args, **kwargs):
            sql=args[args.index('-e')+1]
            out='year=2022\nyear=2023\n' if 'SHOW PARTITIONS' in sql else '3\n'
            return subprocess.CompletedProcess(args,0,out,'')
        with patch.object(mod.shutil,'which',return_value='/fake/beeline'), patch.object(mod.subprocess,'run',side_effect=command):
            result=mod.main(['--jdbc-url','jdbc:hive2://localhost:10000/default','--min-rows','3','--expect-partition','year=2022','--expect-partition','year=2023'])
        self.assertEqual(result,0)
    def test_missing_executable(self):
        with patch.object(mod.shutil,'which',return_value=None):
            self.assertEqual(mod.main(['--jdbc-url','jdbc:hive2://localhost:10000/default']),2)
    def test_empty_partition_fails(self):
        def command(args, **kwargs):
            sql=args[args.index('-e')+1]
            out='year=2022\n' if 'SHOW PARTITIONS' in sql else ('0\n' if 'WHERE' in sql else '3\n')
            return subprocess.CompletedProcess(args,0,out,'')
        with patch.object(mod.shutil,'which',return_value='/fake/beeline'), patch.object(mod.subprocess,'run',side_effect=command):
            result=mod.main(['--jdbc-url','jdbc:hive2://localhost:10000/default','--expect-partition','year=2022'])
        self.assertEqual(result,1)

if __name__=='__main__': unittest.main()
