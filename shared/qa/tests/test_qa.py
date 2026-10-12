#!/usr/bin/env python3
"""Offline tests. Mocks never connect to Hadoop or SSH; all writes use TemporaryDirectory."""

import argparse
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location("qa", ROOT / "shared/qa/check_cluster.py")
qa = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(qa)

REPORT = """Configured Capacity: 3000 (2.93 KB)
Present Capacity: 3000 (2.93 KB)
Under replicated blocks: 0
Blocks with corrupt replicas: 0
Missing blocks: 0
Missing blocks (with replication factor 1): 0
Pending deletion blocks: 0
-------------------------------------------------
Live datanodes (3):
""" + "\n".join(f"""Name: 10.0.0.{i}:9866 (dn{i})
Hostname: dn{i}
Decommission Status : Normal
Configured Capacity: 1000 (1000 B)
""" for i in range(1, 4))
FSCK = """Status: HEALTHY
 Total size: 42 B
 Under-replicated blocks: 0 (0.0 %)
 Mis-replicated blocks: 0 (0.0 %)
 Missing blocks: 0
 Corrupt blocks: 0
The filesystem under path '/' is HEALTHY
"""
NODES = """Total Nodes:3
         Node-Id             Node-State Node-Http-Address Number-of-Running-Containers
dn1:1234 RUNNING dn1:8042 0
dn2:1234 RUNNING dn2:8042 0
dn3:1234 RUNNING dn3:8042 0
"""

MOCK = r'''#!/usr/bin/python3
import json, os, pathlib, sys, time
name = pathlib.Path(sys.argv[0]).name
args = sys.argv[1:]
root = pathlib.Path(os.environ['MOCK_ROOT'])
scenario = os.environ.get('SCENARIO', '')
with (root / 'calls').open('a') as f:
    f.write(json.dumps([name, *args]) + '\n')
if scenario == 'timeout':
    time.sleep(5)
if scenario == 'exit':
    print('Connection refused', file=sys.stderr)
    sys.exit(1)
if name in ('hdfs', 'yarn'):
    if args == ['version']:
        print('Hadoop 3.4.0')
    elif args[:2] == ['getconf', '-confKey']:
        print('file:///' if scenario == 'localfs' else 'hdfs://nn:9000')
    elif args == ['dfsadmin', '-report']:
        print((root / 'report').read_text())
    elif args == ['dfsadmin', '-safemode', 'get']:
        print('Safe mode is ON' if scenario == 'safemode' else 'Safe mode is OFF')
    elif args == ['fsck', '/']:
        print((root / 'fsck').read_text())
    elif args == ['node', '-list', '-all']:
        print((root / 'nodes').read_text())
    elif args[:2] == ['dfs', '-mkdir']:
        if scenario == 'collision': sys.exit(1)
        (root / 'owned').write_text(args[2])
    elif args[:2] == ['dfs', '-chmod']:
        if scenario == 'chmod': sys.exit(1)
    elif args[:2] == ['dfs', '-put']:
        (root / 'payload').write_bytes(pathlib.Path(args[2]).read_bytes())
        if scenario == 'put': sys.exit(1)
    elif args[:2] == ['dfs', '-cat']:
        sys.stdout.buffer.write(b'wrong' if scenario == 'mismatch' else (root / 'payload').read_bytes())
    elif args[:2] in (['dfs', '-rm'], ['dfs', '-rmdir']):
        if scenario == 'cleanup': sys.exit(1)
        owned = (root / 'owned').read_text()
        assert args[-1] in (owned, owned + '/payload')
        assert '-r' not in args and '-R' not in args
    else:
        raise AssertionError(args)
elif name == 'curl':
    url = args[-1]
    is_ui = '/jmx?' not in url and '/ws/' not in url
    if scenario == 'html':
        body = '<html>Login</html>'
    elif is_ui and scenario == 'foreign-ui':
        body = '<html><title>Welcome</title><body>Unrelated service</body></html>'
    elif is_ui and scenario == 'login-ui':
        body = '<html><title>NameNode ResourceManager NodeManager JobHistory</title><body>Cluster Applications Login<input type="password"></body></html>'
    elif '/jmx?' in url:
        age = -1 if scenario == 'no-checkpoint' else 999999999 if scenario == 'stale' else 1000
        bean = {'name': url.split('/jmx?qry=')[1]}
        if scenario != 'missing-checkpoint':
            bean['LastCheckpointDeltaMs'] = age
        if scenario == 'wrong-jmx':
            bean['name'] = 'Hadoop:service=Other,name=Other'
        body = json.dumps({'beans': [bean]})
    elif url.endswith('/ws/v1/cluster/info'):
        body = json.dumps({'clusterInfo': {'state': 'STARTED', 'haState': 'STANDBY' if scenario == 'standby' else 'ACTIVE'}})
    elif url.endswith('/ws/v1/cluster/nodes'):
        body = json.dumps({'nodes': {'node': [{'state': 'LOST' if scenario == 'lost' else 'RUNNING', 'nodeHTTPAddress': f'dn{i}:8042'} for i in range(1,4)]}})
    elif url.endswith('/ws/v1/node/info'):
        body = json.dumps({'nodeInfo': {'nodeHealthy': scenario != 'unhealthy'}})
    elif url.endswith('/ws/v1/history/info'):
        body = json.dumps({'historyInfo': {'hadoopVersion': '3.4.0', 'startedOn': 1000}})
    else:
        pages = {'/dfshealth.html': 'Namenode information', '/status.html': 'SecondaryNameNode',
                 '/datanode.html': 'Datanode information', '/cluster': 'Cluster All Applications',
                 '/node': 'NodeManager information', '/jobhistory': 'JobHistory'}
        path = '/' + url.rsplit('/', 1)[1]
        body = '<html><title>' + pages[path] + '</title><body>Hadoop</body></html>'
    print(body)
    print('302' if scenario == 'redirect' else '401' if scenario == 'unauthorized' else '200', end='')
elif name == 'ssh':
    sys.exit(255 if scenario == 'sshfail' else 0)
else:
    raise AssertionError(name)
'''


class ParserTests(unittest.TestCase):
    def test_report(self):
        qa.parse_report(REPORT)
        for value in ("", REPORT.replace('(3)', '(2)'), REPORT + '\nDead datanodes (1):',
                      REPORT.replace('Normal', 'Decommission in progress', 1),
                      REPORT.replace('replicas: 0', 'replicas: 1'),
                      REPORT.replace('1000 (1000 B)', '0 (0 B)', 1),
                      REPORT.replace('10.0.0.2', '10.0.0.1')):
            with self.subTest(value=value), self.assertRaises(qa.Failed):
                qa.parse_report(value)

    def test_fsck(self):
        qa.parse_fsck(FSCK)
        for value in ('', FSCK.replace('HEALTHY', 'CORRUPT'),
                      FSCK.replace('Under-replicated blocks: 0', 'Under-replicated blocks: 1'),
                      FSCK + 'Permission denied'):
            with self.subTest(value=value), self.assertRaises(qa.Failed):
                qa.parse_fsck(value)

    def test_nodes(self):
        self.assertEqual(len(qa.parse_nodes(NODES, 3)), 3)
        for value in ('', NODES.replace('RUNNING', 'UNHEALTHY', 1),
                      NODES.replace('Total Nodes:3', 'Total Nodes:2'),
                      NODES.replace('dn2:1234', 'dn1:1234')):
            with self.subTest(value=value), self.assertRaises(qa.Failed):
                qa.parse_nodes(value, 3)

    def test_endpoint_rejects_unsafe_values(self):
        for url in ('http://0.0.0.0:80', 'http://host', 'http://u:p@host:80',
                    'http://host:80/extra', 'http://-oProxyCommand:80',
                    'file:///etc/passwd', 'http://host:99999', 'http://host:80?x=1'):
            with self.subTest(url=url), self.assertRaises(ValueError):
                qa.endpoint(url)


class OfflineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='qa-tests-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        for name in ('hdfs', 'yarn', 'curl', 'ssh'):
            path = self.bin / name
            path.write_text(MOCK)
            path.chmod(0o700)
        for name, text in [('report', REPORT), ('fsck', FSCK), ('nodes', NODES)]:
            (self.root / name).write_text(text)
        self.config = json.loads((ROOT / 'shared/qa/cluster.example.json').read_text())
        for index, name in enumerate(self.config['datanodes'], 1):
            self.config['services'][name]['url'] = f'http://dn{index}:9864'
        for index, name in enumerate(self.config['nodemanagers'], 1):
            self.config['services'][name]['url'] = f'http://dn{index}:8042'
        self.config_path = self.root / 'config.json'
        self.save_config()
        self.env = dict(os.environ, MOCK_ROOT=str(self.root), PATH=str(self.bin),
                        HADOOP_HOME=str(self.root), SCENARIO='')

    def save_config(self):
        self.config_path.write_text(json.dumps(self.config))

    def run_qa(self, mode='hdfs', *args, scenario='', expected=0):
        result = subprocess.run([sys.executable, str(ROOT / 'shared/qa/check_cluster.py'),
                                 mode, '--config', str(self.config_path), *args],
                                env=dict(self.env, SCENARIO=scenario), capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
        self.assertNotIn('Traceback', result.stderr)
        return result

    def calls(self):
        path = self.root / 'calls'
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def test_read_only_hdfs(self):
        self.run_qa()
        self.assertFalse(any(c[1:2] == ['dfs'] for c in self.calls()))

    def test_yarn(self):
        self.run_qa('yarn')
        self.assertEqual([c[1:] for c in self.calls() if c[0] == 'yarn'], [['version'], ['node', '-list', '-all']])

    def test_hdfs_failures(self):
        for scenario in ('exit', 'safemode', 'stale', 'html', 'redirect', 'unauthorized'):
            with self.subTest(scenario=scenario):
                self.run_qa(scenario=scenario, expected=1)

    def test_yarn_failures(self):
        for scenario in ('exit', 'standby', 'lost', 'unhealthy', 'html', 'redirect'):
            with self.subTest(scenario=scenario):
                self.run_qa('yarn', scenario=scenario, expected=1)

    def test_no_checkpoint(self):
        result = self.run_qa(scenario='no-checkpoint')
        self.assertIn('PASS: SecondaryNameNode JMX', result.stdout)
        self.assertIn('NOT TESTED (optional): SecondaryNameNode checkpoint', result.stdout)
        self.assertNotIn('PASS: SecondaryNameNode checkpoint', result.stdout)
        self.run_qa('hdfs', '--smoke', '--smoke-base', '/user/test', scenario='no-checkpoint')
        self.assertTrue(any('-put' in c for c in self.calls()))

    def test_pending_deletion_is_informational(self):
        (self.root / 'report').write_text(REPORT.replace('Pending deletion blocks: 0', 'Pending deletion blocks: 5'))
        result = self.run_qa()
        self.assertIn('INFO: Pending deletion blocks: 5', result.stdout)

    def test_web_rejects_foreign_or_login_pages(self):
        # Healthy APIs must not hide an unrelated/login HTML page returning 200.
        for mode in ('hdfs', 'yarn'):
            for scenario in ('foreign-ui', 'login-ui'):
                with self.subTest(mode=mode, scenario=scenario):
                    result = self.run_qa(mode, scenario=scenario, expected=1)
                    self.assertNotRegex(result.stdout, r'PASS: .* Web UI')
        result = self.run_qa(scenario='wrong-jmx', expected=1)
        self.assertNotIn('PASS: namenode Web UI', result.stdout)

    def test_missing_dependencies(self):
        for binary in self.bin.iterdir():
            binary.unlink()
        self.run_qa(expected=2)
        self.run_qa('yarn', expected=2)

    def test_bash_entrypoints(self):
        for script in ('hw01-hdfs/scripts/check-hdfs.sh', 'hw02-yarn/scripts/check-yarn.sh',
                       'hw02-yarn/scripts/open-web-ui.sh'):
            with self.subTest(script=script):
                result = subprocess.run(['/bin/bash', str(ROOT / script), '--help'],
                                        capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                result = subprocess.run(['/bin/bash', str(ROOT / script)],
                                        env=self.env, capture_output=True, text=True)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertIn('NOT TESTED', result.stderr)

    def test_bad_json(self):
        self.config_path.write_text('{broken')
        self.run_qa(expected=2)
        self.assertEqual(self.calls(), [])

    def test_unexpected_node_addresses(self):
        (self.root / 'nodes').write_text(NODES.replace('dn1:8042', 'other:8042'))
        self.run_qa('yarn', expected=1)

    def test_no_smoke_when_diagnostics_incomplete(self):
        self.run_qa('hdfs', '--smoke', '--smoke-base', '/user/test', scenario='missing-checkpoint', expected=2)
        self.assertFalse(any(c[1:2] == ['dfs'] for c in self.calls()))

    def test_missing_config_fields(self):
        self.config = {}
        self.save_config()
        self.run_qa(expected=2)
        self.run_qa('yarn', expected=2)

    def test_bad_config(self):
        for change in ({'jobhistory_required': 'false'}, {'nodemanagers': []},
                       {'checkpoint_max_age_seconds': -1}, {'typo': 1}):
            original = self.config.copy()
            self.config.update(change)
            self.save_config()
            self.run_qa(expected=2)
            self.config = original

    def test_smoke(self):
        self.run_qa('hdfs', '--smoke', '--smoke-base', '/user/test')
        calls = self.calls()
        owned = (self.root / 'owned').read_text()
        self.assertRegex(owned, r'^/user/test/qa-hdfs-[a-f0-9]{32}$')
        self.assertIn(['hdfs', 'dfs', '-rm', '-f', '-skipTrash', owned + '/payload'], calls)
        self.assertIn(['hdfs', 'dfs', '-rmdir', owned], calls)
        self.assertFalse(any('-r' in c or '-R' in c or '-p' in c for c in calls))

    def test_failed_smoke_cleans_owned_data(self):
        for scenario in ('chmod', 'put', 'mismatch', 'cleanup'):
            with self.subTest(scenario=scenario):
                (self.root / 'calls').write_text('')
                result = self.run_qa('hdfs', '--smoke', '--smoke-base', '/user/test', scenario=scenario, expected=1)
                self.assertNotIn('PASS: HDFS запись', result.stdout)
                self.assertTrue(any(c[1:2] == ['dfs'] and '-rm' in c for c in self.calls()))

    def test_collision_never_deletes(self):
        self.run_qa('hdfs', '--smoke', '--smoke-base', '/user/test', scenario='collision', expected=1)
        self.assertFalse(any('-rm' in c or '-rmdir' in c or '-put' in c for c in self.calls()))

    def test_localfs_never_writes(self):
        self.run_qa('hdfs', '--smoke', '--smoke-base', '/user/test', scenario='localfs', expected=1)
        self.assertFalse(any(c[1:2] == ['dfs'] for c in self.calls()))

    def test_smoke_bad_args(self):
        for args in (['--smoke'], ['--smoke', '--smoke-base', '/'],
                     ['--smoke', '--smoke-base', '/user/../test'],
                     ['--smoke', '--smoke-base', '/user/*'], ['--smoke-base', '/user/test'],
                     ['--timeout', '0'], ['--unknown']):
            with self.subTest(args=args):
                self.run_qa('hdfs', *args, expected=2)
        self.assertEqual(self.calls(), [])

    def test_tunnel_dry_run(self):
        result = self.run_qa('tunnel', '--edge', 'student@edge', '--service', 'namenode', '--dry-run')
        self.assertIn('127.0.0.1:19870:', result.stdout)
        self.assertIn('StrictHostKeyChecking=yes', result.stdout)
        self.assertEqual(self.calls(), [])

    def test_tunnel_ssh_error_propagated(self):
        self.run_qa('tunnel', '--edge', 'student@edge', '--service', 'namenode', scenario='sshfail', expected=255)

    def test_tunnel_identity_file(self):
        identity = self.root / 'custom key;literal'
        identity.write_text('test placeholder, not a private key')
        args = ('--edge', 'student@edge', '--service', 'namenode', '--identity-file', str(identity))
        self.run_qa('tunnel', *args, '--dry-run')
        self.assertEqual(self.calls(), [])
        self.run_qa('tunnel', *args)
        ssh = self.calls()[-1]
        self.assertEqual(ssh[ssh.index('-i') + 1], str(identity))
        self.assertIn('IdentitiesOnly=yes', ssh)

    def test_tunnel_missing_identity(self):
        for path in (self.root / 'absent', self.root):
            with self.subTest(path=path):
                self.run_qa('tunnel', '--edge', 'student@edge', '--service', 'namenode',
                            '--identity-file', str(path), '--dry-run', expected=2)
        self.assertEqual(self.calls(), [])

    def test_tunnel_invalid(self):
        for args in (['--edge', '-evil'], ['--edge', 'u@host'],
                     ['--edge', 'u@host', '--service', 'unknown'],
                     ['--edge', 'u@host', '--service', 'namenode', '--ssh-port', '0'],
                     ['--edge', 'u@host', '--service', 'namenode', '--service', 'namenode']):
            with self.subTest(args=args):
                self.run_qa('tunnel', *args, expected=2)
        self.assertEqual(self.calls(), [])

    def test_duplicate_tunnel_port(self):
        self.config['services']['secondarynamenode']['local_port'] = 19870
        self.save_config()
        self.run_qa('tunnel', '--edge', 'u@host', '--service', 'namenode', '--service', 'secondarynamenode', expected=2)
        self.assertEqual(self.calls(), [])

    def test_timeout(self):
        args = argparse.Namespace(timeout=1)
        runner = qa.QA(args, {})
        with patch.dict(os.environ, dict(self.env, SCENARIO='timeout'), clear=True):
            with self.assertRaisesRegex(qa.Failed, 'таймаут'):
                runner.command([str(self.bin / 'hdfs'), 'version'])

    def test_interrupt_smoke_cleanup(self):
        runner = qa.QA(argparse.Namespace(smoke_base='/user/test'), {})
        calls = []

        def cli(*args):
            calls.append(args)
            if '-put' in args:
                raise KeyboardInterrupt()
            return ''

        with patch.object(runner, 'cli', cli), self.assertRaises(KeyboardInterrupt):
            runner.smoke()
        self.assertTrue(any('-rm' in c for c in calls))
        self.assertTrue(any('-rmdir' in c for c in calls))


if __name__ == '__main__':
    unittest.main(verbosity=2)
