"""Teste de integrare: pornesc brokerul real (C#) și rulează scenarii cu clienții Python.

Rulare (din directorul clients/):  python -m unittest discover -s tests -v
Variabila BROKER_DLL poate indica alt Broker.dll (implicit: build Release din broker/src/Broker).
"""
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common.client import BrokerClient  # noqa: E402
from sender import publish  # noqa: E402

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DLL = os.environ.get("BROKER_DLL") or os.path.join(ROOT, "broker", "src", "Broker", "bin", "Release", "net8.0", "Broker.dll")


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class BrokerProc:
    def __init__(self, **cfg):
        self.port = free_port()
        self.cfg = {"Port": self.port, "Host": "127.0.0.1", "HealthPrefix": f"http://localhost:{free_port()}/",
                    "AckTimeoutMs": 500, "RetryBaseMs": 100, "RetryMaxMs": 400, "MaxAttempts": 3,
                    "WorkerIntervalMs": 20, **cfg}
        self.proc = None

    def start(self):
        env = dict(os.environ)
        for k, v in self.cfg.items():
            env[f"BROKER__{k.upper()}"] = str(v)
        self.proc = subprocess.Popen(["dotnet", DLL], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        deadline = time.time() + 15
        while time.time() < deadline:
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=0.5).close()
                return self
            except OSError:
                time.sleep(0.1)
        raise RuntimeError("brokerul nu a pornit")

    def start_cluster(self, cfg):
        """Pornește nodul; cheile 'A__B' devin variabila BROKER__A__B."""
        self.cfg = cfg
        return self.start()

    def kill(self):
        if self.proc:
            self.proc.kill()
            self.proc.wait()
            self.proc = None

    def stop(self):
        self.kill()

    def client(self, cid, role):
        c = BrokerClient("127.0.0.1", self.port, cid, role)
        c.connect()
        return c


def subscribe(c, topic, group=None):
    f = {"action": "SUBSCRIBE", "topic": topic}
    if group:
        f["group"] = group
    c.send(f)
    r = c.recv(timeout=5)
    assert r["action"] == "SUBSCRIBED", r


def next_deliver(c, timeout=5):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            f = c.recv(timeout=max(0.05, deadline - time.time()))
        except socket.timeout:
            return None
        if f is None:
            return None
        if f.get("action") == "DELIVER":
            return f
    return None


def ack(c, f):
    c.send({"action": "ACK", "id": f["id"], "topic": f["topic"]})


class BrokerTests(unittest.TestCase):
    def setUp(self):
        self.broker = BrokerProc().start()
        self.clients = []

    def tearDown(self):
        for c in self.clients:
            c.close()
        self.broker.stop()

    def new(self, cid, role):
        c = self.broker.client(cid, role)
        self.clients.append(c)
        return c

    # --- livrare de bază ---
    def test_publish_deliver_ack(self):
        sub = self.new("r1", "subscriber")
        subscribe(sub, "orders")
        pub = self.new("s1", "publisher")
        payload = {"orderId": 7, "note": "ăîșțâ ✓", "nested": {"a": [1, 2.50, None]}}
        reply = publish(pub, "orders", payload, "OrderCreated")
        self.assertEqual(reply["action"], "PUBLISHED")
        f = next_deliver(sub)
        self.assertIsNotNone(f)
        self.assertEqual(f["payload"], payload)  # mesajul nu e alterat
        self.assertEqual(f["type"], "OrderCreated")
        self.assertEqual(f["attempt"], 1)
        ack(sub, f)
        self.assertIsNone(next_deliver(sub, 1.5))  # după ACK nu mai e retrimis

    # --- mesaje invalide: brokerul nu cade ---
    def test_invalid_messages_do_not_crash_broker(self):
        pub = self.new("s1", "publisher")
        cases = [
            ("{nu e json", "INVALID_JSON"),
            ("[1,2,3]", "INVALID_JSON"),
            ('{"action":"PUBLISH","id":5}', "INVALID_JSON"),
            ('{"id":"x"}', "MISSING_FIELD"),
            ('{"action":"FOO"}', "UNKNOWN_ACTION"),
            ('{"action":"PUBLISH","id":"a","topic":"t"}', "MISSING_FIELD"),
            ('{"action":"PUBLISH","id":"a","topic":"$dlq","payload":1}', "FORBIDDEN_TOPIC"),
            ('{"action":"PUBLISH","id":"a","topic":"bad topic!","payload":1}', "INVALID_FIELD"),
            ('{"action":"SUBSCRIBE","topic":"t"}', "FORBIDDEN_ROLE"),
            ('{"action":"HELLO","clientId":"x","role":"publisher"}', "DUPLICATE_HELLO"),
        ]
        for raw, code in cases:
            pub.send_raw(raw + "\n")
            r = pub.recv(timeout=3)
            self.assertEqual((r["action"], r["code"]), ("ERROR", code), raw)
        # conexiunea e încă utilizabilă
        self.assertEqual(publish(pub, "t", {"ok": True})["action"], "PUBLISHED")

    def test_first_frame_must_be_hello(self):
        s = socket.create_connection(("127.0.0.1", self.broker.port))
        s.sendall(b'{"action":"PING"}\n')
        s.settimeout(3)
        data = s.makefile("rb").readline()
        self.assertEqual(json.loads(data)["code"], "NOT_HELLO")
        s.close()
        self.new("later", "publisher")  # brokerul merge în continuare

    def test_frame_too_large(self):
        b = BrokerProc(MaxFrameBytes=2048).start()
        try:
            c = b.client("big", "publisher")
            c.send_raw('{"action":"PUBLISH","id":"1","topic":"t","payload":"' + "x" * 5000 + '"}\n')
            self.assertEqual(c.recv(timeout=3)["code"], "FRAME_TOO_LARGE")
            self.assertEqual(publish(c, "t", {"small": 1})["action"], "PUBLISHED")
            c.close()
        finally:
            b.stop()

    # --- backlog, grupuri, duplicate ---
    def test_backlog_delivered_to_first_subscriber(self):
        pub = self.new("s1", "publisher")
        for i in range(3):
            publish(pub, "late", {"i": i})
        sub = self.new("r1", "subscriber")
        subscribe(sub, "late")
        got = sorted(next_deliver(sub)["payload"]["i"] for _ in range(3))
        self.assertEqual(got, [0, 1, 2])

    def test_fanout_between_groups_and_competition_within_group(self):
        a1, a2 = self.new("a1", "subscriber"), self.new("a2", "subscriber")
        b1 = self.new("b1", "subscriber")
        for c, g in ((a1, "A"), (a2, "A"), (b1, "B")):
            subscribe(c, "news", g)
        pub = self.new("s1", "publisher")
        n = 20
        for i in range(n):
            publish(pub, "news", {"i": i})
        count = {"a1": 0, "a2": 0, "b1": 0}

        def drain(name, c):  # în paralel, altfel ACK-urile ajung după timeout și mesajele sunt retrimise
            while True:
                f = next_deliver(c, 1.0)
                if not f:
                    break
                ack(c, f)
                count[name] += 1

        ths = [threading.Thread(target=drain, args=(n, c)) for n, c in (("a1", a1), ("a2", a2), ("b1", b1))]
        [t.start() for t in ths]
        [t.join() for t in ths]
        self.assertEqual(count["b1"], n)                    # grupul B primește toate mesajele
        self.assertEqual(count["a1"] + count["a2"], n)      # grupul A își împarte mesajele
        self.assertTrue(count["a1"] > 0 and count["a2"] > 0)

    def test_duplicate_publish_is_idempotent(self):
        sub = self.new("r1", "subscriber")
        subscribe(sub, "dup")
        pub = self.new("s1", "publisher")
        publish(pub, "dup", {"x": 1}, msg_id="same-id")
        r = publish(pub, "dup", {"x": 1}, msg_id="same-id")
        self.assertTrue(r.get("duplicate"))
        ack(sub, next_deliver(sub))
        self.assertIsNone(next_deliver(sub, 1.0))

    # --- politici de livrare ---
    def test_retry_then_dead_letter(self):
        sub = self.new("r1", "subscriber")
        subscribe(sub, "jobs")
        dlq = self.new("dlq-reader", "subscriber")
        subscribe(dlq, "$dlq")
        pub = self.new("s1", "publisher")
        publish(pub, "jobs", {"job": 1}, msg_id="job-1")
        attempts = []
        while True:  # nu confirmăm niciodată
            f = next_deliver(sub, 4)
            if not f:
                break
            attempts.append(f["attempt"])
        self.assertEqual(attempts, [1, 2, 3])               # MaxAttempts=3
        d = next_deliver(dlq, 3)
        self.assertIsNotNone(d)
        self.assertEqual(d["payload"]["originalId"], "job-1")
        self.assertEqual(d["payload"]["payload"], {"job": 1})

    def test_receiver_crash_redelivers_to_other_member(self):
        r1, r2 = self.new("r1", "subscriber"), self.new("r2", "subscriber")
        subscribe(r1, "w", "g"), subscribe(r2, "w", "g")
        pub = self.new("s1", "publisher")
        for i in range(4):
            publish(pub, "w", {"i": i})
        r1.close()  # r1 cade fără să confirme
        seen = set()
        deadline = time.time() + 5
        while len(seen) < 4 and time.time() < deadline:
            f = next_deliver(r2, 1.0)
            if f:
                seen.add(f["payload"]["i"])
                ack(r2, f)
        self.assertEqual(seen, {0, 1, 2, 3})

    def test_ttl_expired_message_is_dropped(self):
        pub = self.new("s1", "publisher")
        publish(pub, "ttl", {"x": 1}, ttl_ms=200)
        time.sleep(0.5)
        sub = self.new("r1", "subscriber")
        subscribe(sub, "ttl")
        self.assertIsNone(next_deliver(sub, 1.0))

    # --- concurență ---
    def test_concurrent_publishers_no_loss_no_duplicates(self):
        sub = self.new("r1", "subscriber")
        subscribe(sub, "load")
        threads_n, per = 8, 100
        errors = []

        def run(t):
            try:
                c = self.broker.client(f"p{t}", "publisher")
                for i in range(per):
                    publish(c, "load", {"t": t, "i": i}, msg_id=f"{t}-{i}")
                c.close()
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        ths = [threading.Thread(target=run, args=(t,)) for t in range(threads_n)]
        [t.start() for t in ths]
        got = {}
        deadline = time.time() + 30
        while len(got) < threads_n * per and time.time() < deadline:
            f = next_deliver(sub, 2)
            if f:
                got[f["id"]] = got.get(f["id"], 0) + 1
                ack(sub, f)
        [t.join() for t in ths]
        self.assertEqual(errors, [])
        self.assertEqual(len(got), threads_n * per)


class PersistenceTests(unittest.TestCase):
    def test_unacked_messages_survive_broker_crash(self):
        data = tempfile.mkdtemp()
        b = BrokerProc(Storage="file", DataDir=data).start()
        try:
            pub = b.client("s1", "publisher")
            for i in range(5):
                publish(pub, "durable", {"i": i}, msg_id=f"m{i}")
            pub.close()
            b.kill()  # cădere bruscă (fără oprire curată)

            b2 = BrokerProc(Storage="file", DataDir=data)
            b2.port = b.port
            b2.cfg["Port"] = b.port
            b2.start()
            try:
                sub = b2.client("r1", "subscriber")
                subscribe(sub, "durable")
                got = set()
                for _ in range(5):
                    f = next_deliver(sub, 5)
                    self.assertIsNotNone(f)
                    got.add(f["payload"]["i"])
                    ack(sub, f)
                self.assertEqual(got, set(range(5)))
                time.sleep(0.5)  # ACK-urile se jurnalizează asincron; lăsăm brokerul să le scrie înainte de kill
                sub.close()
            finally:
                b2.stop()

            # după ACK, la o nouă repornire nu mai există nimic de livrat
            b3 = BrokerProc(Storage="file", DataDir=data)
            b3.port = b.port
            b3.cfg["Port"] = b.port
            b3.start()
            try:
                sub = b3.client("r1", "subscriber")
                subscribe(sub, "durable")
                self.assertIsNone(next_deliver(sub, 1.5))
                sub.close()
            finally:
                b3.stop()
        finally:
            b.stop()
            shutil.rmtree(data, ignore_errors=True)


def http_get(port, path, timeout=1.0):
    import urllib.request
    try:
        with urllib.request.urlopen(f"http://localhost:{port}{path}", timeout=timeout) as r:
            return r.read().decode()
    except Exception:  # noqa: BLE001
        return None


def wait_for(cond, timeout=15, step=0.2):
    deadline = time.time() + timeout
    while time.time() < deadline:
        v = cond()
        if v:
            return v
        time.sleep(step)
    return None


class ClusterTests(unittest.TestCase):
    """Două noduri broker (primary/standby) pe aceeași mașină."""

    def make_nodes(self):
        self.cport = [free_port(), free_port()]
        self.rport = [free_port(), free_port()]
        self.hport = [free_port(), free_port()]
        peers = f"broker-0@127.0.0.1:{self.rport[0]},broker-1@127.0.0.1:{self.rport[1]}"
        self.nodes = []
        for i in range(2):
            n = BrokerProc(Cluster__Enabled="true", Cluster__NodeId=f"broker-{i}", Cluster__ReplPort=self.rport[i],
                           Cluster__Peers=peers, Cluster__FailoverMs=1500, Cluster__ProbeIntervalMs=200,
                           Cluster__HeartbeatMs=200, HealthPrefix=f"http://localhost:{self.hport[i]}/")
            n.port = self.cport[i]
            n.cfg["Port"] = self.cport[i]
            self.nodes.append(n)

    def start_node(self, i):
        env_cfg = self.nodes[i].cfg
        # cheile imbricate folosesc "__" în variabila de mediu: BROKER__CLUSTER__ENABLED
        self.nodes[i].start_cluster(env_cfg)

    def role(self, i):
        return http_get(self.hport[i], "/role")

    def setUp(self):
        self.make_nodes()

    def tearDown(self):
        for n in self.nodes:
            n.stop()

    def test_failover_preserves_messages_and_old_leader_rejoins_as_standby(self):
        self.start_node(0)
        self.start_node(1)
        self.assertTrue(wait_for(lambda: self.role(0) == "leader" and self.role(1) == "standby"),
                        f"roluri: {self.role(0)}/{self.role(1)}")

        # standby refuză clienții (conexiunea e închisă imediat)
        s = socket.create_connection(("127.0.0.1", self.cport[1]))
        s.settimeout(2)
        self.assertEqual(s.recv(10), b"")
        s.close()

        # mesaje publicate la lider, fără abonat; se replică la standby
        c = BrokerClient("127.0.0.1", self.cport[0], "s1", "publisher")
        c.connect()
        for i in range(5):
            publish(c, "ha", {"i": i}, msg_id=f"ha-{i}")
        c.close()
        time.sleep(1.0)  # lasă replicarea asincronă să ajungă

        self.nodes[0].kill()  # cade liderul
        self.assertTrue(wait_for(lambda: self.role(1) == "leader", timeout=10), "standby nu s-a promovat")

        sub = BrokerClient("127.0.0.1", self.cport[1], "r1", "subscriber")
        sub.connect()
        subscribe(sub, "ha")
        got = set()
        for _ in range(5):
            f = next_deliver(sub, 5)
            self.assertIsNotNone(f, f"primite doar {sorted(got)}")
            got.add(f["payload"]["i"])
            ack(sub, f)
        self.assertEqual(got, set(range(5)))
        sub.close()

        # vechiul lider revine: găsește un lider și devine standby (nu preemptează)
        self.start_node(0)
        self.assertTrue(wait_for(lambda: self.role(0) == "standby", timeout=10), f"rol nod 0: {self.role(0)}")
        self.assertEqual(self.role(1), "leader")


if __name__ == "__main__":
    unittest.main()
