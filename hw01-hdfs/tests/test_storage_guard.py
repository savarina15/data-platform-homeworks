"""Safety regressions: a format must never replace an existing HDFS cluster."""

import importlib.util
import tempfile
import unittest
from pathlib import Path

SPEC = importlib.util.spec_from_file_location(
    "storage_guard", Path(__file__).parents[1] / "scripts" / "inspect-storage.py"
)
guard = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(guard)


class StorageGuardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def metadata(self, role, name, cluster="CID-original"):
        directory = self.root / name
        current = directory / "current"
        current.mkdir(parents=True)
        storage_type = "DATA_NODE" if role == "datanode" else "NAME_NODE"
        properties = f"storageType={storage_type}\nclusterID={cluster}\nlayoutVersion=-67\ncTime=0\n"
        if role != "datanode":
            properties += "namespaceID=123\nblockpoolID=BP-original\n"
            (current / "fsimage_0000000000000000000").write_bytes(b"image")
            (current / "fsimage_0000000000000000000.md5").write_text("checksum")
        (current / "VERSION").write_text(properties)
        return directory

    def report(self, role, directory, running=()):
        return {"storage": {role: guard.inspect_directory(role, directory)}, "running_daemons": list(running)}

    def test_new_empty_cluster_can_be_initialized(self):
        nn = self.root / "nn"
        nn.mkdir()
        reports = {"nn": self.report("namenode", nn), "dn": self.report("datanode", self.root / "missing")}
        self.assertFalse(guard.validate_cluster(reports)["formatted"])

    def test_nonempty_directory_without_version_is_rejected(self):
        (self.root / "important-data").write_text("preserve me")
        with self.assertRaisesRegex(ValueError, "nonempty storage"):
            guard.inspect_directory("namenode", self.root)

    def test_formatted_running_cluster_is_preserved(self):
        reports = {
            "nn": self.report("namenode", self.metadata("namenode", "nn"), ["namenode"]),
            "dn": self.report("datanode", self.metadata("datanode", "dn"), ["datanode"]),
        }
        self.assertEqual(guard.validate_cluster(reports), {"formatted": True, "cluster_id": "CID-original"})

    def test_datanode_metadata_blocks_new_namenode_format(self):
        reports = {
            "nn": self.report("namenode", self.root / "new-nn"),
            "dn": self.report("datanode", self.metadata("datanode", "dn")),
        }
        with self.assertRaisesRegex(ValueError, "existing metadata"):
            guard.validate_cluster(reports)

    def test_checkpoint_metadata_blocks_new_namenode_format(self):
        reports = {
            "nn": self.report("namenode", self.root / "new-nn"),
            "sn": self.report("checkpoint", self.metadata("checkpoint", "sn")),
        }
        with self.assertRaisesRegex(ValueError, "existing metadata"):
            guard.validate_cluster(reports)

    def test_different_cluster_id_is_rejected(self):
        reports = {
            "nn": self.report("namenode", self.metadata("namenode", "nn")),
            "dn": self.report("datanode", self.metadata("datanode", "dn", "CID-other")),
        }
        with self.assertRaisesRegex(ValueError, "clusterID differs"):
            guard.validate_cluster(reports)

    def test_running_daemon_blocks_initial_format(self):
        reports = {"nn": self.report("namenode", self.root / "new-nn", ["namenode"])}
        with self.assertRaisesRegex(ValueError, "daemons are running"):
            guard.validate_cluster(reports)

    def test_incomplete_namenode_metadata_is_rejected(self):
        path = self.metadata("namenode", "nn")
        (path / "current" / "fsimage_0000000000000000000.md5").unlink()
        with self.assertRaisesRegex(ValueError, "no fsimage"):
            guard.inspect_directory("namenode", path)

    def test_corrupt_version_is_rejected(self):
        path = self.metadata("namenode", "nn")
        (path / "current" / "VERSION").write_text("clusterID=CID-original\nstorageType=NAME_NODE\n")
        with self.assertRaises(ValueError):
            guard.inspect_directory("namenode", path)

    def test_symlink_storage_is_rejected(self):
        link = self.root / "link"
        link.symlink_to(self.root, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "non-symlink"):
            guard.inspect_directory("namenode", link)

    def test_new_datanode_can_join_existing_cluster(self):
        reports = {
            "nn": self.report("namenode", self.metadata("namenode", "nn")),
            "dn": self.report("datanode", self.root / "new-dn"),
        }
        self.assertTrue(guard.validate_cluster(reports)["formatted"])


if __name__ == "__main__":
    unittest.main()
