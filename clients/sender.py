"""Sender (publisher): serializează mesajul în JSON și îl trimite brokerului.

Exemple:
  python sender.py --topic orders --payload '{"orderId": 1, "total": 25.5}'
  python sender.py --topic orders --count 1000 --threads 4          # generator de mesaje
  python sender.py --topic orders --raw '{nu e json valid'          # test mesaj invalid
"""
import argparse
import itertools
import logging
import os
import socket
import threading
import time
import uuid
from datetime import datetime, timezone

from common.client import BrokerClient, BrokerError

log = logging.getLogger("sender")


def publish(client, topic, payload, msg_type=None, ttl_ms=None, msg_id=None, stop=None):
    """Publică un mesaj și așteaptă confirmarea PUBLISHED. La cădere reconectează și retrimite cu ACELAȘI id
    (brokerul deduplică), deci nu se pierd și nu se dublează mesaje."""
    msg_id = msg_id or uuid.uuid4().hex
    frame = {
        "action": "PUBLISH",
        "id": msg_id,
        "topic": topic,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }
    if msg_type:
        frame["type"] = msg_type
    if ttl_ms:
        frame["ttlMs"] = ttl_ms

    while True:
        try:
            client.send(frame)
            while True:
                reply = client.recv(timeout=10)
                if reply is None:
                    raise ConnectionError("conexiune închisă de broker")
                action = reply.get("action")
                if action == "PUBLISHED" and reply.get("id") == msg_id:
                    return reply
                if action == "ERROR" and reply.get("id") in (msg_id, None):
                    raise BrokerError(reply.get("code"), reply.get("reason"), reply.get("id"))
        except (OSError, ConnectionError, socket.timeout) as e:
            log.warning("publicare întreruptă (%s) -> reconectare și retrimitere %s", e, msg_id)
            if not client.connect_with_retry(stop=stop):
                raise


def worker(args, thread_no, counter, total):
    client = BrokerClient(args.host, args.port, f"{args.client_id}-{thread_no}", "publisher")
    client.connect_with_retry()
    sent = 0
    interval = 1.0 / args.rate if args.rate else 0
    while True:
        n = next(counter)
        if n >= total:
            break
        payload = {"seq": n, "sender": client.client_id, "data": args.data}
        try:
            publish(client, args.topic, payload, args.type, args.ttl_ms)
            sent += 1
        except BrokerError as e:
            log.error("mesaj %d respins: %s", n, e)
        if interval:
            time.sleep(interval)
    client.close()
    log.info("thread %d: %d mesaje trimise", thread_no, sent)


def main():
    p = argparse.ArgumentParser(description="Sender pentru brokerul de mesaje")
    p.add_argument("--host", default=os.environ.get("BROKER_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("BROKER_PORT", "5000")))
    p.add_argument("--client-id", default=os.environ.get("CLIENT_ID", f"sender-{uuid.uuid4().hex[:6]}"))
    p.add_argument("--topic", required=True)
    p.add_argument("--type", help="tipul mesajului (informativ)")
    p.add_argument("--payload", help="payload JSON, ex: '{\"a\":1}'")
    p.add_argument("--raw", help="trimite text brut (necodat/invalid) pentru teste")
    p.add_argument("--ttl-ms", type=int)
    p.add_argument("--count", type=int, default=1, help="câte mesaje se trimit (mod generator)")
    p.add_argument("--threads", type=int, default=1)
    p.add_argument("--rate", type=float, default=0, help="mesaje/secundă per thread (0 = maxim)")
    p.add_argument("--data", default="hello", help="câmpul 'data' al mesajelor generate")
    p.add_argument("--forever", action="store_true", help="generează la nesfârșit (pentru demo K8s)")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s", datefmt="%H:%M:%S")

    if args.raw is not None:
        c = BrokerClient(args.host, args.port, args.client_id, "publisher")
        c.connect()
        c.send_raw(args.raw + "\n")
        print("răspuns broker:", c.recv(timeout=5))
        c.close()
        return

    if args.payload is not None:
        import json
        try:
            payload = json.loads(args.payload)
        except ValueError as e:
            raise SystemExit(f"--payload nu este JSON valid: {e}")
        c = BrokerClient(args.host, args.port, args.client_id, "publisher")
        c.connect_with_retry()
        print(publish(c, args.topic, payload, args.type, args.ttl_ms))
        c.close()
        return

    total = float("inf") if args.forever else args.count
    counter = itertools.count()
    t0 = time.time()
    threads = [threading.Thread(target=worker, args=(args, i, counter, total)) for i in range(args.threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    dt = time.time() - t0
    if not args.forever:
        log.info("gata: %d mesaje în %.2fs (%.0f msg/s)", args.count, dt, args.count / dt if dt else 0)


if __name__ == "__main__":
    main()
