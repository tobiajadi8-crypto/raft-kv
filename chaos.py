"""Three independent OS processes with persistent stores. Only kills children it created."""
import argparse
import hashlib
import http.client
import json
import platform
import socket
import statistics
import subprocess
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

def ports(n):
    sockets = [socket.socket() for _ in range(n)]
    try:
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()

def eventually(check, seconds=20):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            value = check()
            if value is not None and value is not False:
                return value
        except (OSError, http.client.HTTPException):
            pass
        time.sleep(0.025)
    raise TimeoutError(f"condition failed: {check}")

class Cluster:
    def __init__(self, binary, root):
        self.binary, self.root = binary, root
        addresses = ports(6)
        self.http, self.raft = addresses[:3], addresses[3:]
        self.nodes = {}

    def start(self, i):
        peers = ",".join(f"n{j}=127.0.0.1:{port}" for j, port in enumerate(self.raft))
        with (self.root / f"n{i}.log").open("ab") as log:
            self.nodes[i] = subprocess.Popen([self.binary, "-id", f"n{i}", "-http",
                f"127.0.0.1:{self.http[i]}", "-peers", peers, "-data", self.root / str(i)],
                stdin=subprocess.DEVNULL, stdout=log, stderr=log)

    def kill(self, i):
        proc = self.nodes.pop(i)
        proc.kill()
        proc.wait(timeout=10)

    def request(self, i, method, path, body=None):
        conn = http.client.HTTPConnection("127.0.0.1", self.http[i], timeout=3)
        try:
            conn.request(method, path, body)
            response = conn.getresponse()
            return response.status, response.read()
        finally:
            conn.close()

    def leader(self):
        for i in list(self.nodes):
            try:
                status, body = self.request(i, "GET", "/status")
                if status == 200 and json.loads(body)["role"] == "Leader":
                    return i
            except (OSError, http.client.HTTPException):
                continue
        return None

    def verify(self, expected):
        leader = eventually(self.leader)
        for key, value in expected.items():
            status, body = self.request(leader, "GET", "/kv/" + key)
            assert status == 200 and json.loads(body)["value"] == value, key
        digest = hashlib.sha256(json.dumps(expected, separators=(",", ":"), sort_keys=True).encode()).hexdigest()
        for i in self.nodes:
            eventually(lambda: json.loads(self.request(i, "GET", "/debug/digest")[1])["sha256"] == digest)

def trial(cluster, count):
    for i in range(3):
        cluster.start(i)
    leader = eventually(cluster.leader)
    expected = {f"k{i}": "x" * 64 for i in range(count)}
    def put(key):
        assert cluster.request(leader, "PUT", "/kv/" + key, expected[key])[0] == 204
    start = time.perf_counter()
    with ThreadPoolExecutor(8) as pool:
        list(pool.map(put, expected))
    rate = count / (time.perf_counter() - start)
    # Keep one logical writer active across the crash. Retry the same key/value
    # after ambiguous responses. Record a key only after its PUT returned 204.
    active, killed = threading.Event(), threading.Event()
    def write_through_failure():
        index = 0
        while True:
            key = f"live{index}"
            after_crash = killed.is_set()
            try:
                node = cluster.leader()
                if node is not None and cluster.request(node, "PUT", "/kv/" + key, "live")[0] == 204:
                    expected[key] = "live"
                    active.set()
                    if after_crash:
                        return time.perf_counter()
                    index += 1
                    continue
            except (OSError, http.client.HTTPException):
                pass
            if time.perf_counter() > deadline:
                raise TimeoutError("no write recovered")
            time.sleep(0.025)
    deadline = time.perf_counter() + 30
    with ThreadPoolExecutor(1) as pool:
        writer = pool.submit(write_through_failure)
        assert active.wait(10)
        start = time.perf_counter()
        cluster.kill(leader)
        killed.set()
        downtime = writer.result() - start
    new = eventually(cluster.leader)
    # More writes than the trailing-log window, forcing snapshot catch-up.
    for i in range(80):
        key = f"tail{i}"
        assert cluster.request(new, "PUT", "/kv/" + key, "catchup")[0] == 204
        expected[key] = "catchup"
    assert cluster.request(new, "POST", "/snapshot")[0] == 204
    cluster.start(leader)
    cluster.verify(expected)
    eventually(lambda: any((cluster.root / str(leader) / "snapshots").glob("*/meta.json")))
    for i in list(cluster.nodes):
        cluster.kill(i)
    for i in range(3):
        cluster.start(i)
    cluster.verify(expected)
    return {"steady_writes_s": round(rate, 2), "failover_s": round(downtime, 3),
            "verified_keys": len(expected)}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--writes", type=int, default=500)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    if min(args.writes, args.trials) < 1:
        parser.error("writes and trials must be positive")
    results = []
    for _ in range(args.trials):
        with tempfile.TemporaryDirectory() as directory:
            cluster = Cluster(args.binary.resolve(), Path(directory))
            try:
                results.append(trial(cluster, args.writes))
            except Exception:
                for log in cluster.root.glob("*.log"):
                    print(log.name, log.read_text(errors="replace")[-1800:])
                raise
            finally:
                for i in list(cluster.nodes):
                    cluster.kill(i)
    report = dict(platform=platform.platform(), writes_per_trial=args.writes, concurrency=8,
                  trials=results, median_writes_s=statistics.median(r["steady_writes_s"] for r in results),
                  median_failover_s=statistics.median(r["failover_s"] for r in results))
    print(json.dumps(report, indent=2))
    if args.report:
        args.report.write_text(json.dumps(report, indent=2) + "\n")
