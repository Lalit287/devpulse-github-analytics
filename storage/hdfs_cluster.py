"""Manage only DevPulse's isolated, loopback-only NameNode and DataNode."""
import argparse
import getpass
import json
import os
import shutil
import signal
import socket
import subprocess
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import requests

from config.settings import ROOT
from exploration.io import write_json
from exploration.spark_demo import configure_java

PROFILE = os.environ.get("DEVPULSE_HDFS_PROFILE", "development")
if PROFILE not in {"development", "verification"}:
    raise ValueError("DEVPULSE_HDFS_PROFILE must be development or verification")
_PROFILE_SUFFIX = "-verification" if PROFILE == "verification" else ""
CONF_DIR = ROOT / f"config/hadoop{_PROFILE_SUFFIX}"
RUNTIME = ROOT / f".runtime/hdfs{_PROFILE_SUFFIX}"
PORTS = ({"rpc": 19100, "namenode_http": 19970, "datanode_http": 19964,
          "datanode_data": 19966, "datanode_ipc": 19967} if PROFILE == "verification" else
         {"rpc": 19000, "namenode_http": 19870, "datanode_http": 19864,
          "datanode_data": 19866, "datanode_ipc": 19867})
HDFS_URI = f"hdfs://127.0.0.1:{PORTS['rpc']}"


def hadoop_home():
    explicit = os.environ.get("DEVPULSE_HADOOP_HOME")
    executable = shutil.which("hadoop")
    candidates = [Path(explicit)] if explicit else []
    if executable:
        candidates.append(Path(executable).resolve().parents[1])
    candidates.append(Path("/opt/hadoop"))
    for candidate in candidates:
        if (candidate / "bin/hdfs").is_file():
            return candidate
    raise RuntimeError("Hadoop is not installed; set DEVPULSE_HADOOP_HOME to an installation")


def hadoop_environment():
    configure_java()
    env = os.environ.copy()
    env.update({"HADOOP_HOME": str(hadoop_home()), "HADOOP_CONF_DIR": str(CONF_DIR),
                "HADOOP_LOG_DIR": str(RUNTIME / "logs"), "HADOOP_PID_DIR": str(RUNTIME / "pids"),
                "HADOOP_IDENT_STRING": "devpulse", "HADOOP_USER_NAME": getpass.getuser(),
                "HADOOP_HEAPSIZE_MAX": "512", "HADOOP_OPTS": "",
                "HDFS_NAMENODE_OPTS": "", "HDFS_DATANODE_OPTS": ""})
    return env


def write_configuration(directory=CONF_DIR, runtime=RUNTIME):
    directory, runtime = Path(directory), Path(runtime).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    for subdir in ("namenode", "datanode", "tmp", "logs", "pids"):
        (runtime / subdir).mkdir(parents=True, exist_ok=True)
    configurations = {
        "core-site.xml": {"fs.defaultFS": HDFS_URI, "hadoop.tmp.dir": str(runtime / "tmp"),
                          "hadoop.security.authentication": "simple"},
        "hdfs-site.xml": {
            "dfs.replication": "1", "dfs.namenode.name.dir": (runtime / "namenode").as_uri(),
            "dfs.datanode.data.dir": (runtime / "datanode").as_uri(),
            "dfs.namenode.rpc-address": f"127.0.0.1:{PORTS['rpc']}",
            "dfs.namenode.rpc-bind-host": "127.0.0.1",
            "dfs.namenode.http-address": f"127.0.0.1:{PORTS['namenode_http']}",
            "dfs.datanode.address": f"127.0.0.1:{PORTS['datanode_data']}",
            "dfs.datanode.http.address": f"127.0.0.1:{PORTS['datanode_http']}",
            "dfs.datanode.ipc.address": f"127.0.0.1:{PORTS['datanode_ipc']}",
            "dfs.datanode.hostname": "127.0.0.1", "dfs.client.use.datanode.hostname": "true",
            "dfs.datanode.use.datanode.hostname": "true", "dfs.webhdfs.enabled": "true",
            "dfs.namenode.safemode.min.datanodes": "1", "dfs.namenode.safemode.extension": "1000",
            "dfs.namenode.datanode.registration.ip-hostname-check": "false",
        },
    }
    for name, properties in configurations.items():
        root = ET.Element("configuration")
        for key, value in properties.items():
            prop = ET.SubElement(root, "property")
            ET.SubElement(prop, "name").text = key
            ET.SubElement(prop, "value").text = value
        ET.indent(root, space="  ")
        ET.ElementTree(root).write(directory / name, encoding="utf-8", xml_declaration=True)
    # Reuse only logging configuration; global filesystem/service settings stay untouched.
    try:
        installed = hadoop_home() / "etc/hadoop"
        for name in ("log4j.properties", "log4j2.properties"):
            if (installed / name).is_file():
                shutil.copyfile(installed / name, directory / name)
    except RuntimeError:
        pass  # Configuration generation is also usable before Hadoop installation.
    return configurations


def run_hdfs(arguments, *, timeout=120, check=True):
    command = [str(hadoop_home() / "bin/hdfs"), "--config", str(CONF_DIR), *arguments]
    result = subprocess.run(command, env=hadoop_environment(), capture_output=True, text=True, timeout=timeout)
    if check and result.returncode:
        raise RuntimeError(f"hdfs command failed ({result.returncode}): {result.stderr.strip()}")
    return result


def jmx(port, query):
    with requests.Session() as session:
        session.trust_env = False
        response = session.get(f"http://127.0.0.1:{port}/jmx", params={"qry": query}, timeout=3)
        response.raise_for_status()
        return response.json()["beans"][0]


def health():
    bean = jmx(PORTS["namenode_http"], "Hadoop:service=NameNode,name=FSNamesystemState")
    return {"namenode": "healthy", "live_datanodes": int(bean["NumLiveDataNodes"]),
            "profile": PROFILE,
            "hdfs_uri": HDFS_URI, "ports": PORTS, "configuration_dir": str(CONF_DIR),
            "runtime_dir": str(RUNTIME)}


def matching_process(entry):
    try:
        name = jmx(entry["http_port"], "java.lang:type=Runtime")["Name"]
        return int(str(name).split("@", 1)[0]) == entry["pid"]
    except (requests.RequestException, KeyError, IndexError, ValueError):
        return False


def format_if_new():
    version = RUNTIME / "namenode/current/VERSION"
    if version.exists():
        return False
    if any((RUNTIME / "namenode").iterdir()) or any((RUNTIME / "datanode").iterdir()):
        raise RuntimeError("Refusing to format a nonempty unrecognized HDFS directory")
    run_hdfs(["namenode", "-format", "-nonInteractive", "-clusterid", "CID-devpulse-week2"])
    if not version.exists():
        raise RuntimeError("NameNode format did not produce VERSION")
    return True


def start_cluster(timeout=60, *, evidence_path=None):
    state_path = RUNTIME / "processes.json"
    if state_path.exists():
        entries = json.loads(state_path.read_text())
        if entries and all(matching_process(entry) for entry in entries):
            result = health()
            if result["live_datanodes"] == 1:
                return {**result, "reused_running_cluster": True}
            raise RuntimeError("Existing DevPulse cluster is not healthy; inspect logs before restart")
        if any(matching_process(entry) for entry in entries):
            raise RuntimeError("Partially running DevPulse cluster; stop it before starting")
    for port in PORTS.values():
        with socket.socket() as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                probe.bind(("127.0.0.1", port))
            except OSError as exc:
                raise RuntimeError(f"Loopback port {port} is unavailable: {exc}") from exc
    write_configuration()
    formatted = format_if_new()
    processes, entries = [], []
    try:
        for daemon, port in (("namenode", PORTS["namenode_http"]), ("datanode", PORTS["datanode_http"])):
            log_path = RUNTIME / "logs" / f"{daemon}.log"
            with log_path.open("ab") as handle:
                process = subprocess.Popen(
                    [str(hadoop_home() / "bin/hdfs"), "--config", str(CONF_DIR), daemon],
                    env=hadoop_environment(), stdin=subprocess.DEVNULL, stdout=handle,
                    stderr=subprocess.STDOUT, start_new_session=True)
            processes.append(process)
            entries.append({"daemon": daemon, "pid": process.pid, "http_port": port})
        write_json(state_path, entries)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if any(process.poll() is not None for process in processes):
                raise RuntimeError("HDFS daemon exited; inspect .runtime/hdfs/logs")
            try:
                result = health()
                if result["live_datanodes"] == 1 and all(matching_process(entry) for entry in entries):
                    run_hdfs(["dfsadmin", "-safemode", "wait"], timeout=15)
                    result.update({"formatted_new_cluster": formatted, "reused_running_cluster": False})
                    evidence_name = "verification_hdfs_environment.json" if PROFILE == "verification" else "hdfs_environment.json"
                    write_json(evidence_path or ROOT / "reports/week2" / evidence_name, result)
                    return result
            except (requests.RequestException, KeyError, IndexError):
                pass
            time.sleep(.5)
        raise RuntimeError("HDFS startup timed out; inspect .runtime/hdfs/logs")
    except BaseException:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        raise


def stop_cluster():
    state_path = RUNTIME / "processes.json"
    if not state_path.exists():
        return {"status": "already stopped"}
    entries = json.loads(state_path.read_text())
    stopped = []
    for entry in reversed(entries):
        # JMX's JVM PID proves identity before any signal; never kill an arbitrary PID.
        if matching_process(entry):
            os.kill(entry["pid"], signal.SIGTERM)
            stopped.append(entry["daemon"])
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        ports_released = True
        for port in PORTS.values():
            with socket.socket() as probe:
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                try:
                    probe.bind(("127.0.0.1", port))
                except OSError:
                    ports_released = False
        if not any(matching_process(entry) for entry in entries) and ports_released:
            write_json(state_path, [])
            return {"status": "stopped", "daemons": stopped}
        time.sleep(.25)
    raise RuntimeError("HDFS graceful stop timed out; inspect logs. No forced PID termination was performed.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["configure", "start", "status", "stop"])
    args = parser.parse_args()
    try:
        if args.action == "configure":
            write_configuration()
            result = {"configuration": str(CONF_DIR), "hdfs_uri": HDFS_URI}
        elif args.action == "start":
            result = start_cluster()
        elif args.action == "stop":
            result = stop_cluster()
        else:
            result = health()
        print(json.dumps(result, indent=2))
    except (OSError, ValueError, RuntimeError, requests.RequestException, subprocess.TimeoutExpired) as exc:
        parser.exit(1, f"HDFS {args.action} failed: {type(exc).__name__}: {exc}\n")


if __name__ == "__main__":
    main()
