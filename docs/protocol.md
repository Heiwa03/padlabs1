# Specificația protocolului

Protocol de aplicație peste **TCP**. Un cadru = **un obiect JSON pe o linie**, terminat cu `\n` (NDJSON, UTF-8).
Dimensiune maximă a unei linii: `MaxFrameBytes` (implicit 1 MB). Un cadru mai mare este respins cu `FRAME_TOO_LARGE`,
iar conexiunea rămâne utilizabilă.

Câmpul `payload` este **opac**: brokerul îl stochează ca text JSON brut și îl retransmite neschimbat.

## Sesiune

1. Clientul deschide conexiunea TCP și trimite imediat `HELLO` (altfel `NOT_HELLO` / `TIMEOUT` și conexiunea se închide).
2. Un `role` per conexiune: **publisher** (poate doar `PUBLISH`) sau **subscriber** (poate `SUBSCRIBE`, `UNSUBSCRIBE`, `ACK`, `NACK`).
   Canalele sunt astfel unidirecționale: publisher → broker → subscriber. Un client care are nevoie de ambele deschide două conexiuni.
3. `PING` → `PONG` menține conexiunea (conexiunile inactive mai mult de `IdleTimeoutMs` sunt închise).

## Cadre client → broker

| action | câmpuri | observații |
|---|---|---|
| `HELLO` | `clientId`, `role` | `clientId`: `[A-Za-z0-9._-/]{1,128}` |
| `PUBLISH` | `id`, `topic`, `payload`, `type?`, `timestamp?`, `ttlMs?` | `id` unic per topic (deduplicare); topicurile `$…` sunt rezervate |
| `SUBSCRIBE` | `topic`, `group?` | `group` implicit = `clientId` |
| `UNSUBSCRIBE` | `topic` | |
| `ACK` | `id`, `topic` | confirmă procesarea unui `DELIVER` |
| `NACK` | `id`, `topic` | refuz explicit → retry cu backoff |
| `PING` | – | |

## Cadre broker → client

| action | câmpuri | când |
|---|---|---|
| `WELCOME` | `clientId`, `role` | după `HELLO` |
| `PUBLISHED` | `id`, `topic`, `duplicate?` | mesajul este stocat (durabil în mod `file`) |
| `SUBSCRIBED` / `UNSUBSCRIBED` | `topic`, `group?` | |
| `DELIVER` | `id`, `topic`, `group`, `type?`, `timestamp?`, `payload`, `attempt` | livrare (at-least-once) |
| `ERROR` | `code`, `reason`, `id?` | cadru respins; conexiunea rămâne deschisă (except `NOT_HELLO`, `TIMEOUT`) |
| `PONG` | – | |

## Exemple

```json
{"action":"HELLO","clientId":"sender-1","role":"publisher"}
{"action":"WELCOME","clientId":"sender-1","role":"publisher"}
{"action":"PUBLISH","id":"8f1c…","topic":"orders","type":"OrderCreated","timestamp":"2026-10-07T14:54:58+00:00","payload":{"orderId":42,"total":99.5}}
{"action":"PUBLISHED","id":"8f1c…","topic":"orders"}
```
```json
{"action":"HELLO","clientId":"receiver-A","role":"subscriber"}
{"action":"SUBSCRIBE","topic":"orders","group":"workers"}
{"action":"SUBSCRIBED","topic":"orders","group":"workers"}
{"action":"DELIVER","id":"8f1c…","topic":"orders","group":"workers","type":"OrderCreated","payload":{"orderId":42,"total":99.5},"attempt":1}
{"action":"ACK","id":"8f1c…","topic":"orders"}
```

## Coduri de eroare

| cod | cauză |
|---|---|
| `INVALID_JSON` | linia nu este JSON valid, nu este obiect sau are tipuri greșite |
| `MISSING_FIELD` / `INVALID_FIELD` | câmp obligatoriu lipsă / valoare invalidă |
| `UNKNOWN_ACTION` | acțiune necunoscută |
| `FRAME_TOO_LARGE` | linie > `MaxFrameBytes` |
| `NOT_HELLO`, `DUPLICATE_HELLO` | ordinea sesiunii |
| `FORBIDDEN_ROLE` | acțiune nepermisă rolului (ex. `PUBLISH` de la un subscriber) |
| `FORBIDDEN_TOPIC` | publicare pe un topic rezervat (`$dlq`) |
| `NOT_SUBSCRIBED`, `ALREADY_SUBSCRIBED` | stare de abonare incompatibilă |
| `QUEUE_FULL` | coada unui grup a atins `MaxGroupQueue` (backpressure către publisher) |
| `TIMEOUT` | `HELLO` nesosit la timp / conexiune inactivă |
| `INTERNAL` | eroare internă (de ex. scriere eșuată în jurnal); mesajul NU a fost stocat |

## Semantica rutării

* **Topic** = identificatorul logic al canalului; publisherul nu cunoaște receiverii.
* **Grup** (`group`): membrii aceluiași grup își împart mesajele (round-robin, *competing consumers*); grupuri diferite primesc
  fiecare câte o copie (*fan-out*). Implicit fiecare receiver are grupul propriu (= primește tot). „Unul-la-unu” = un singur abonat.
* Un topic fără abonați păstrează mesajele într-un **backlog**; primul grup care se abonează le primește.
* Grupurile sunt durabile: mesajele rămân în coadă cât timp grupul nu are membri conectați (până la `ttlMs` sau `MaxGroupQueue`).

## Politici de livrare

| situație | comportament |
|---|---|
| fără `ACK` în `AckTimeoutMs` | retrimitere cu backoff exponențial (`RetryBaseMs`·2^(n-1), max `RetryMaxMs`) |
| după `MaxAttempts` încercări | mesajul se mută în topicul `$dlq` (se poate abona oricine) |
| receiver cade | mesajele lui in-flight revin imediat în coadă și merg la ceilalți membri ai grupului |
| `PUBLISH` repetat cu același `id` | confirmat idempotent (`duplicate:true`), nu se dublează |
| `ttlMs` depășit | mesajul este șters, nu se livrează |
| broker cade (`Storage=file`) | la repornire mesajele neconfirmate se reîncarcă din jurnal |
| broker cade (cluster) | standby-ul devine lider cu starea replicată |

Garanție: **at-least-once** — un mesaj poate fi livrat de mai multe ori (după timeout/cădere), niciodată pierdut după `PUBLISHED`
(în mod `file`, modulo ferestra de replicare asincronă la failover). Receiverii trebuie să fie idempotenți (după `id`).

## Protocol intern de replicare (cluster)

Pe un port separat (`ReplPort`, implicit 5001), același format NDJSON:
`{"op":"who"}` / `{"op":"sync",…}` → `{"op":"role","role":"leader|standby",…}`; liderul trimite apoi
`snap_begin`, înregistrările snapshot, `snap_end`, apoi fluxul de înregistrări WAL (`pub`/`ack`/`grp`) și `hb` (heartbeat).
