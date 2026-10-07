"""Receiver (subscriber): se abonează la unul sau mai multe topicuri, afișează mesajele și trimite ACK.

Exemple:
  python receiver.py --topic orders
  python receiver.py --topic orders --group workers          # mai mulți receiveri cu același group își împart mesajele
  python receiver.py --topic orders --no-ack-rate 0.3        # simulează căderi: 30% din mesaje nu sunt confirmate
  python receiver.py --topic '$dlq'                          # citește mesajele moarte (dead letter)
"""
import argparse
import json
import logging
import os
import random
import socket
import time
import uuid

from common.client import BrokerClient, BrokerError

log = logging.getLogger("receiver")


def run(args):
    client = BrokerClient(args.host, args.port, args.client_id, "subscriber")
    received = 0
    while True:
        client.connect_with_retry()
        try:
            for topic in args.topic:
                frame = {"action": "SUBSCRIBE", "topic": topic}
                if args.group:
                    frame["group"] = args.group
                client.send(frame)
            consume(client, args)
        except (OSError, ConnectionError, json.JSONDecodeError) as e:
            log.warning("conexiune pierdută (%s) -> reconectare", e)
            time.sleep(0.5)
        except KeyboardInterrupt:
            client.close()
            return


def consume(client, args):
    last_ping = time.time()
    while True:
        try:
            frame = client.recv(timeout=args.ping_interval)
        except socket.timeout:
            client.send({"action": "PING"})
            continue
        if frame is None:
            raise ConnectionError("conexiune închisă de broker")

        action = frame.get("action")
        if action == "DELIVER":
            on_deliver(client, frame, args)
        elif action == "SUBSCRIBED":
            log.info("abonat: topic=%s group=%s", frame.get("topic"), frame.get("group"))
        elif action == "ERROR":
            log.error("eroare de la broker: %s %s", frame.get("code"), frame.get("reason"))
        # PONG și restul se ignoră


def on_deliver(client, frame, args):
    log.info("[%s] id=%s attempt=%s payload=%s", frame.get("topic"), frame.get("id"), frame.get("attempt"),
             json.dumps(frame.get("payload"), ensure_ascii=False))
    if args.delay:
        time.sleep(args.delay)
    r = random.random()
    if r < args.no_ack_rate:
        log.warning("  (simulare) nu confirm mesajul %s", frame.get("id"))
        return
    if r < args.no_ack_rate + args.nack_rate:
        log.warning("  (simulare) NACK pentru %s", frame.get("id"))
        client.send({"action": "NACK", "id": frame["id"], "topic": frame["topic"]})
        return
    client.send({"action": "ACK", "id": frame["id"], "topic": frame["topic"]})


def main():
    p = argparse.ArgumentParser(description="Receiver pentru brokerul de mesaje")
    p.add_argument("--host", default=os.environ.get("BROKER_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("BROKER_PORT", "5000")))
    p.add_argument("--client-id", default=os.environ.get("CLIENT_ID", f"receiver-{uuid.uuid4().hex[:6]}"))
    p.add_argument("--topic", action="append", required=True, help="poate fi repetat")
    p.add_argument("--group", default=os.environ.get("GROUP"),
                   help="grup de abonați (implicit = clientId => fiecare receiver primește toate mesajele)")
    p.add_argument("--delay", type=float, default=0, help="secunde de procesare simulată per mesaj")
    p.add_argument("--no-ack-rate", type=float, default=0, help="probabilitatea de a NU confirma (test retry/DLQ)")
    p.add_argument("--nack-rate", type=float, default=0, help="probabilitatea de a trimite NACK")
    p.add_argument("--ping-interval", type=float, default=15)
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s", datefmt="%H:%M:%S")
    try:
        run(args)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
