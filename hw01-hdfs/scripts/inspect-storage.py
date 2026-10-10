#!/usr/bin/env python3
"""Read-only HDFS storage inspection and cluster-wide format guard."""

import argparse
import json
import subprocess
import sys
from pathlib import Path


def inspect_directory(role, directory):
    path = Path(directory)
    result = {"path": str(path), "state": "missing"}
    if not path.is_absolute() or path.is_symlink():
        raise ValueError(f"{role}: expected an absolute, non-symlink directory: {path}")
    if not path.exists():
        return result
    if not path.is_dir():
        raise ValueError(f"{role}: not a directory: {path}")
    if not any(path.iterdir()):
        result["state"] = "empty"
        return result
    version = path / "current" / "VERSION"
    if not version.is_file() or version.is_symlink():
        raise ValueError(f"{role}: nonempty storage without a regular current/VERSION: {path}")
    properties = {}
    for line in version.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith(("#", "!")) and "=" in line:
            key, value = line.split("=", 1)
            properties[key.strip()] = value.strip()
    expected_type = "DATA_NODE" if role == "datanode" else "NAME_NODE"
    if properties.get("storageType") != expected_type or not properties.get("clusterID"):
        raise ValueError(f"{role}: invalid storageType or clusterID in {version}")
    required_numbers = ["layoutVersion", "cTime"]
    if role != "datanode":
        required_numbers.append("namespaceID")
        if not properties.get("blockpoolID"):
            raise ValueError(f"{role}: missing blockpoolID in {version}")
    try:
        for key in required_numbers:
            int(properties[key])
    except (KeyError, ValueError) as error:
        raise ValueError(f"{role}: invalid or missing numeric metadata in {version}") from error
    if role != "datanode":
        images = [p for p in version.parent.glob("fsimage_*") if p.name[8:].isdigit() and p.is_file()]
        if not any(Path(str(image) + ".md5").is_file() for image in images):
            raise ValueError(f"{role}: no fsimage with checksum file in {version.parent}")
    result.update(state="formatted", cluster_id=properties["clusterID"])
    return result


def validate_cluster(reports):
    namenodes = [r["storage"]["namenode"] for r in reports.values() if "namenode" in r["storage"]]
    if len(namenodes) != 1:
        raise ValueError("Expected exactly one NameNode storage report")
    namenode = namenodes[0]
    formatted = namenode["state"] == "formatted"
    for host, report in reports.items():
        if not formatted and report.get("running_daemons"):
            raise ValueError(f"{host}: HDFS daemons are running; refusing initial format")
        for role, storage in report["storage"].items():
            if storage["state"] not in ("missing", "empty", "formatted"):
                raise ValueError(f"{host}/{role}: unexpected storage state")
            if not formatted and storage["state"] == "formatted":
                raise ValueError(f"{host}/{role}: existing metadata with unformatted NameNode; refusing format")
            if formatted and storage["state"] == "formatted" and storage["cluster_id"] != namenode["cluster_id"]:
                raise ValueError(f"{host}/{role}: clusterID differs from NameNode; no data will be reset")
    return {"formatted": formatted, "cluster_id": namenode.get("cluster_id")}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for role in ("namenode", "datanode", "checkpoint"):
        parser.add_argument(f"--{role}-dir")
    parser.add_argument("--jps")
    parser.add_argument("--validate-cluster")
    args = parser.parse_args()
    try:
        if args.validate_cluster is not None:
            result = validate_cluster(json.loads(args.validate_cluster))
        else:
            storage = {}
            for role in ("namenode", "datanode", "checkpoint"):
                directory = getattr(args, role + "_dir")
                if directory:
                    storage[role] = inspect_directory(role, directory)
            running = []
            if args.jps:
                processes = subprocess.run([args.jps, "-l"], check=True, text=True, capture_output=True).stdout
                classes = {
                    "namenode": "org.apache.hadoop.hdfs.server.namenode.NameNode",
                    "datanode": "org.apache.hadoop.hdfs.server.datanode.DataNode",
                    "secondarynamenode": "org.apache.hadoop.hdfs.server.namenode.SecondaryNameNode",
                }
                for role, class_name in classes.items():
                    if any(line.split()[1:2] == [class_name] for line in processes.splitlines()):
                        running.append(role)
            result = {"storage": storage, "running_daemons": running}
        print(json.dumps(result))
    except (ValueError, OSError, subprocess.CalledProcessError) as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
