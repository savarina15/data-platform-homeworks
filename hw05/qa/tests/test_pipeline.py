"""Local tests use synthetic responses, never claim real-cluster readiness."""
import argparse
import contextlib
import copy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest

QA_DIR = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("pipeline", QA_DIR / "check_pipeline.py")
qa = importlib.util.module_from_spec(spec)
spec.loader.exec_module(qa)
FLOW = "00000000-0000-4000-8000-000000000001"
RUN = "00000000-0000-4000-8000-000000000002"
APP = "application_1794268800000_0001"
START = 1794268800000


def fixtures():
    return {
        "/api/health": True,
        "/api/flow_runs/" + RUN: {"id": RUN, "flow_id": FLOW, "state_type": "COMPLETED",
            "state": {"type": "COMPLETED"}, "start_time": "2026-11-10T00:00:00Z", "end_time": "2026-11-10T00:01:00Z"},
        "/ws/v1/cluster/info": {"clusterInfo": {"state": "STARTED", "haState": "ACTIVE"}},
        "/ws/v1/cluster/apps/" + APP: {"app": {"id": APP, "applicationType": "SPARK", "state": "FINISHED",
            "finalStatus": "SUCCEEDED", "user": "team", "applicationTags": "prefect-flow-run-id=" + RUN,
            "startedTime": START + 10000, "finishedTime": START + 50000}},
    }


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.config = json.loads((QA_DIR / "pipeline.example.json").read_text())
        self.config["prefect"]["flow_id"] = FLOW
        self.responses = fixtures()
        self.commands = []

    def args(self):
        return argparse.Namespace(timeout=2, flow_run_id=RUN, application_id=APP,
                                  hdfs_bin="hdfs", beeline_bin="beeline", report=None)

    def command(self, argv):
        self.commands.append(argv)
        if argv[0] == "hdfs":
            if argv[2] == "-count":
                return b"3 2 130 hdfs://team-37-nn:9000/user/team/datasets/books\n"
            if argv[2] == "-stat":
                return str(START + 30000).encode() + b"\n"
            self.fail("unexpected HDFS operation: " + str(argv))
        self.assertEqual(argv[0], "beeline")
        sql = argv[-1]
        if sql.startswith("DESCRIBE FORMATTED"):
            return ("Location:\t" + self.config["hive"]["location"] + "\t\n").encode()
        if sql.startswith("DESCRIBE"):
            return b"count\tbigint\t\ngenre\tstring\t\n\n# Partition Information\n# col_name\tdata_type\tcomment\ngenre\tstring\t\n"
        if sql.startswith("SHOW PARTITIONS"):
            return b"genre=Dystopia\ngenre=Fiction\n"
        if sql.startswith("SELECT COUNT(*)"):
            return b"1\n" if "WHERE" in sql else b"2\n"
        if sql.startswith("SELECT `count`"):
            return b"1\tFiction\n1\tDystopia\n"
        self.fail("unexpected SQL: " + sql)

    def checker(self):
        obj = qa.Checks(self.args(), self.config)
        obj.command = self.command
        obj.json = lambda url: copy.deepcopy(self.responses["/" + url.split("/", 3)[3]])
        return obj

    def execute(self, obj=None):
        obj = obj or self.checker()
        with contextlib.redirect_stdout(io.StringIO()):
            code = obj.run()
        return code, obj

    def test_positive_and_repeat_read_only(self):
        for _ in range(2):
            code, obj = self.execute()
            self.assertEqual(code, 0)
            self.assertEqual(len(obj.results), 7)
            self.assertTrue(all(row["status"] == "PASS" for row in obj.results))
        self.assertTrue(self.commands)
        for argv in self.commands:
            if argv[0] == "hdfs":
                self.assertIn(argv[2], ["-count", "-stat"])
            else:
                self.assertTrue(argv[-1].startswith(("SELECT ", "DESCRIBE ", "SHOW PARTITIONS ")))

    def test_failed_flow_and_dependency(self):
        for state in ["FAILED", "RUNNING", "CRASHED", "CANCELLED", "SCHEDULED"]:
            with self.subTest(state=state):
                self.responses["/api/flow_runs/" + RUN]["state_type"] = state
                code, obj = self.execute()
                self.assertEqual(code, 1)
                self.assertEqual(obj.results[4]["status"], "NOT TESTED")
                self.assertEqual(obj.results[-1]["status"], "NOT TESTED")

    def test_wrong_flow_identity_or_missing_times(self):
        run = self.responses["/api/flow_runs/" + RUN]
        for key, value in [("id", FLOW), ("flow_id", RUN), ("start_time", None),
                           ("end_time", "2026-11-09T00:01:00Z"), ("start_time", "2026-11-10T00:00:00")]:
            with self.subTest(key=key, value=value):
                original = run[key]
                run[key] = value
                self.assertEqual(self.execute()[0], 1)
                run[key] = original

    def test_yarn_failure_wrong_app_and_unrelated_run(self):
        app = self.responses["/ws/v1/cluster/apps/" + APP]["app"]
        for key, value in [("id", "application_1_1"), ("applicationType", "MAPREDUCE"),
                ("state", "RUNNING"), ("finalStatus", "FAILED"), ("user", "other"),
                ("applicationTags", "prefect-flow-run-id=" + FLOW),
                ("applicationTags", ""), ("startedTime", START - 10000),
                ("finishedTime", START + 90000), ("startedTime", str(START))]:
            with self.subTest(key=key, value=value):
                original = app[key]
                app[key] = value
                self.assertEqual(self.execute()[0], 1)
                app[key] = original

    def test_missing_run_or_contract_is_not_tested(self):
        for field in ["flow_run_id", "application_id"]:
            obj = self.checker()
            setattr(obj.args, field, None)
            self.assertEqual(self.execute(obj)[0], 2)
        self.config["prefect"]["flow_id"] = None
        self.assertEqual(self.execute()[0], 2)

    def test_service_failure_does_not_hide_other_checks(self):
        obj = self.checker()
        obj.json = lambda _: (_ for _ in ()).throw(qa.Failed("HTTP 503"))
        code, obj = self.execute(obj)
        self.assertEqual(code, 1)
        self.assertEqual(obj.results[1]["status"], "PASS")
        self.assertEqual(obj.results[5]["status"], "PASS")

    def test_missing_tools_not_tested(self):
        obj = self.checker()
        obj.command = lambda _: (_ for _ in ()).throw(qa.NotTested("binary missing"))
        self.assertEqual(self.execute(obj)[0], 2)

    def test_empty_input_and_malformed_count(self):
        for output in [b"3 0 0 /path", b"3 2 0 /path", b"garbage", b""]:
            obj = self.checker()
            obj.command = lambda argv: output if argv[0] == "hdfs" else self.command(argv)
            self.assertEqual(self.execute(obj)[0], 1)

    def test_hive_schema_location_rows_partitions_and_read_failures(self):
        cases = [("DESCRIBE FORMATTED", b"Location:\thdfs://other:9000/result\n"),
                 ("DESCRIBE ", b"count\tstring\t\ngenre\tstring\t\n"),
                 ("SHOW PARTITIONS", b"genre=Fiction\n"),
                 ("SELECT COUNT(*)", b"0\n"), ("SELECT COUNT(*)", b"INFO\n2\n"),
                 ("SELECT COUNT(*)", b"Error: SQL\n2\n"),
                 ("SELECT `count`", b""), ("SELECT `count`", b"2\tFiction\n1\tDystopia\n")]
        for prefix, output in cases:
            with self.subTest(prefix=prefix, output=output):
                obj = self.checker()
                obj.command = lambda argv: output if argv[0] == "beeline" and argv[-1].startswith(prefix) else self.command(argv)
                code, obj = self.execute(obj)
                self.assertEqual(code, 1)
                self.assertEqual(obj.results[5]["status"], "FAIL")
                self.assertEqual(obj.results[6]["status"], "NOT TESTED")

    def test_stale_or_future_files(self):
        for output in [str(START - 100000).encode(), str(START + 100000).encode(), b"", b"bad"]:
            obj = self.checker()
            obj.command = lambda argv: output if argv[0] == "hdfs" and argv[2] == "-stat" else self.command(argv)
            self.assertEqual(self.execute(obj)[0], 1)

    def test_unpartitioned_result(self):
        table = self.config["hive"]
        table["partition_column"] = None
        table["expect_partitions"] = []
        table["expected_rows"] = None
        original = self.command
        def command(argv):
            if argv[0] == "beeline":
                if argv[-1].startswith("DESCRIBE `"):
                    return b"count\tbigint\t\n"
                if argv[-1].startswith("SELECT `count`"):
                    return b"1\n1\n"
            return original(argv)
        obj = self.checker()
        obj.command = command
        self.assertEqual(self.execute(obj)[0], 0)
        self.assertFalse(any("SHOW PARTITIONS" in argv[-1] for argv in self.commands))

    def test_json_report(self):
        obj = self.checker()
        obj.args.report = str(self.directory / "report.json")
        self.assertEqual(self.execute(obj)[0], 0)
        result = json.loads(Path(obj.args.report).read_text())
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["flow_run_id"], RUN)
        self.assertEqual(len(result["checks"]), 7)
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(self.execute(obj)[0], 1)
        self.assertEqual(json.loads(Path(obj.args.report).read_text()), result)

    def save_config(self):
        path = self.directory / "pipeline.json"
        path.write_text(json.dumps(self.config))
        return path

    def test_example_config(self):
        data = qa.config(QA_DIR / "pipeline.example.json")
        self.assertIsNone(data["prefect"]["flow_id"])

    def test_bad_configuration(self):
        mutations = [lambda d: d.update(unknown=1), lambda d: d.update(input="/local"),
            lambda d: d.update(input="hdfs://team-37-nn:9000/path/../secret"),
            lambda d: d.update(clock_skew_seconds=True), lambda d: d.update(clock_skew_seconds=61),
            lambda d: d["prefect"].update(api_url="http://user:password@host:4200/api"),
            lambda d: d["prefect"].update(flow_id="not-uuid"),
            lambda d: d["hive"].update(table="books; DROP TABLE x"),
            lambda d: d["hive"].update(jdbc_url="jdbc:hive2://host:10000/default;password=secret"),
            lambda d: d["hive"].update(location="hdfs://other:9000/path"),
            lambda d: d["hive"].update(columns=[{"name": "genre", "type": "STRING"}]),
            lambda d: d["hive"].update(expect_partitions=["genre=a' OR '1'='1"]),
            lambda d: d["hive"].update(min_rows=False),
            lambda d: d["hive"].update(expected_rows=[["bad\nrow"]])]
        original = copy.deepcopy(self.config)
        for mutation in mutations:
            self.config = copy.deepcopy(original)
            mutation(self.config)
            with self.subTest(config=self.config), self.assertRaises((qa.Failed, qa.hive.QaError, ValueError)):
                qa.config(self.save_config())

    def test_cli_bad_config_missing_file_invalid_ids_and_timeout(self):
        path = self.save_config()
        for argv in [["--config", str(path), "--flow-run-id", "../x"],
                     ["--config", str(path), "--application-id", "application_1_1/../x"],
                     ["--config", str(path), "--timeout", "0"],
                     ["--config", str(self.directory / "absent")]]:
            proc = subprocess.run([sys.executable, str(QA_DIR / "check_pipeline.py"), *argv], capture_output=True, text=True)
            self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)
        path.write_text("{bad json")
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(qa.main(["--config", str(path)]), 2)

    def test_command_timeout_kills_child_group_and_nonzero_fails(self):
        obj = qa.Checks(self.args(), self.config)
        obj.args.timeout = 1
        with self.assertRaises(qa.Failed):
            obj.command([sys.executable, "-c", "raise SystemExit(7)"])
        pidfile = self.directory / "pid"
        source = "import subprocess,time,sys; p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)']);open(sys.argv[1],'w').write(str(p.pid));time.sleep(30)"
        before = time.monotonic()
        with self.assertRaisesRegex(qa.Failed, "таймаут"):
            obj.command([sys.executable, "-c", source, str(pidfile)])
        self.assertLess(time.monotonic() - before, 5)
        child = Path("/proc") / pidfile.read_text() / "stat"
        # Killed orphan may remain briefly as a zombie, but must not be running.
        if child.exists():
            self.assertEqual(child.read_text().split()[2], "Z")
        with self.assertRaises(qa.NotTested):
            obj.command([str(self.directory / "missing")])

    def test_real_local_http_transport(self):
        responses = self.responses
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                status = 302 if self.path == "/redirect" else 503 if self.path == "/unavailable" else 200
                self.send_response(status)
                if status == 302:
                    self.send_header("Location", "/api/health")
                self.end_headers()
                if self.path == "/slow":
                    time.sleep(2)
                    return
                self.wfile.write(b"<html>login</html>" if self.path == "/html" else json.dumps(responses.get(self.path)).encode())
            def log_message(self, *_):
                pass
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        base = f"http://127.0.0.1:{server.server_port}"
        obj = qa.Checks(self.args(), self.config)
        self.assertIs(obj.json(base + "/api/health"), True)
        for path in ["/redirect", "/unavailable", "/html"]:
            with self.subTest(path=path), self.assertRaises((qa.Failed, ValueError)):
                obj.json(base + path)
        obj.args.timeout = 1
        with self.assertRaises(qa.Failed):
            obj.json(base + "/slow")
        port = server.server_port
        server.shutdown()
        server.server_close()
        with self.assertRaises(qa.Failed):
            obj.json(f"http://127.0.0.1:{port}/api/health")

    def test_full_cli_with_local_fake_services(self):
        # Exercises real CLI exit codes and subprocess plumbing; still a local simulation.
        binary = self.directory / "tool"
        binary.write_text("#!/usr/bin/env python3\nimport json,sys,os\nargs=sys.argv\n"
            "if '-count' in args: print('3 2 130 /books')\n"
            f"elif '-stat' in args: print({START + 30000})\n"
            "elif '-e' in args:\n"
            " sql=args[-1]\n"
            " if sql.startswith('DESCRIBE FORMATTED'): print('Location:\\thdfs://team-37-nn:9000/user/hive/warehouse/books_stats_2023')\n"
            " elif sql.startswith('DESCRIBE'): print('count\\tbigint\\t\\ngenre\\tstring\\t')\n"
            " elif sql.startswith('SHOW'): print('genre=Dystopia\\ngenre=Fiction')\n"
            " elif sql.startswith('SELECT COUNT'): print(1 if 'WHERE' in sql else 2)\n"
            " else: print('1\\tDystopia\\n1\\tFiction')\n"
            "else:\n"
            " url=args[-1]; path='/' + url.split('/',3)[3]\n"
            " print(json.dumps(json.loads(os.environ['QA_RESPONSES'])[path])); print('200', end='')\n")
        binary.chmod(0o700)
        (self.directory / "curl").symlink_to(binary)
        env = dict(os.environ, PATH=str(self.directory) + os.pathsep + os.environ["PATH"], QA_RESPONSES=json.dumps(self.responses))
        base = [sys.executable, str(QA_DIR / "check_pipeline.py"), "--config", str(self.save_config()),
                "--flow-run-id", RUN, "--application-id", APP, "--hdfs-bin", str(binary), "--beeline-bin", str(binary)]
        proc = subprocess.run(base, env=env, capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("RESULT: PASS", proc.stdout)
        report = self.directory / "fail.json"
        self.responses["/ws/v1/cluster/apps/" + APP]["app"]["finalStatus"] = "FAILED"
        env["QA_RESPONSES"] = json.dumps(self.responses)
        proc = subprocess.run(base + ["--report", str(report)], env=env, capture_output=True, text=True)
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertEqual(json.loads(report.read_text())["status"], "FAIL")


if __name__ == "__main__":
    unittest.main()
