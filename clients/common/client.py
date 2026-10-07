"""Client minimal pentru protocolul brokerului (TCP + JSON pe linii / NDJSON).

Doar biblioteca standard. Folosit atât de sender.py cât și de receiver.py.
"""
import json
import logging
import random
import socket
import time

log = logging.getLogger("client")


class BrokerError(Exception):
    """Cadru ERROR primit de la broker."""

    def __init__(self, code, reason, frame_id=None):
        super().__init__(f"{code}: {reason}")
        self.code = code
        self.reason = reason
        self.frame_id = frame_id


class BrokerClient:
    def __init__(self, host, port, client_id, role, connect_timeout=5.0):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.role = role  # "publisher" | "subscriber"
        self.connect_timeout = connect_timeout
        self._sock = None
        self._rfile = None

    # ---------- conexiune ----------

    def connect(self):
        """Se conectează și face HELLO/WELCOME. Ridică OSError / BrokerError la eșec."""
        self.close()
        sock = socket.create_connection((self.host, self.port), timeout=self.connect_timeout)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.settimeout(None)
        self._sock = sock
        self._rfile = sock.makefile("rb")
        self.send({"action": "HELLO", "clientId": self.client_id, "role": self.role})
        frame = self.recv(timeout=self.connect_timeout)
        if frame is None or frame.get("action") != "WELCOME":
            raise BrokerError("HANDSHAKE", f"răspuns neașteptat la HELLO: {frame}")

    def connect_with_retry(self, max_backoff=10.0, stop=None):
        """Încearcă la nesfârșit (backoff exponențial cu jitter) până reușește."""
        delay = 0.5
        while stop is None or not stop():
            try:
                self.connect()
                log.info("conectat la %s:%s ca %s (%s)", self.host, self.port, self.client_id, self.role)
                return True
            except (OSError, BrokerError) as e:
                log.warning("conectare eșuată (%s); reîncerc în %.1fs", e, delay)
                time.sleep(delay + random.uniform(0, delay / 4))
                delay = min(delay * 2, max_backoff)
        return False

    def close(self):
        for obj in (self._rfile, self._sock):
            try:
                if obj:
                    obj.close()
            except OSError:
                pass
        self._rfile = None
        self._sock = None

    # ---------- transport ----------

    def send(self, frame):
        data = (json.dumps(frame, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
        self._sock.sendall(data)

    def send_raw(self, text):
        """Trimite text brut (folosit pentru a testa mesaje invalide)."""
        self._sock.sendall(text.encode("utf-8"))

    def recv(self, timeout=None):
        """Citește un cadru. None = conexiune închisă de broker. socket.timeout dacă expiră timeout-ul."""
        self._sock.settimeout(timeout)
        line = self._rfile.readline()
        if not line:
            return None
        return json.loads(line)
