# Agent de mesagerie (Message Broker) — Partea 1: Socket-uri

Broker C# (.NET 8) + clienți Python, protocol JSON pe linii peste TCP, Pub/Sub pe topic, stocare transient/persistent,
cluster primary/standby, Docker și Kubernetes (kind).

* **Documentație completă:** [docs/DOCUMENTATION.md](docs/DOCUMENTATION.md) (sursă pentru prezentare)
* **Specificația protocolului:** [docs/protocol.md](docs/protocol.md)
* **Variantă HTML (cuprins, temă luminoasă/întunecată, diagrame randate):** [docs/DOCUMENTATION.html](docs/DOCUMENTATION.html) — se regenerează cu `pip install markdown && python docs/build_html.py`

```
broker/            C# — src/Broker (broker), tests/Broker.Tests (xUnit), Dockerfile
clients/           Python — sender.py, receiver.py, common/client.py, tests/ (integrare), Dockerfile
deploy/            kind-cluster.yaml, k8s/ (manifeste Kubernetes + demo.ps1)
docker-compose.yml demo local
```

## Pornire rapidă

```powershell
cd broker; dotnet run --project src/Broker                    # broker pe :5000
cd clients; python receiver.py --topic orders                 # terminal 2
cd clients; python sender.py --topic orders --payload '{"a":1}'   # terminal 3
```

Docker: `docker compose up --build` · Kubernetes: `powershell -File deploy/k8s/demo.ps1`

Teste: `cd broker; dotnet test tests/Broker.Tests` și `cd clients; python -m unittest discover -s tests -v`
