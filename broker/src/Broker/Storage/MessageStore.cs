using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using Broker.Config;
using Broker.Network;
using Broker.Protocol;
using Broker.Routing;
using Microsoft.Extensions.Logging;

namespace Broker.Storage;

/// <summary>
/// Unitatea de stocare a brokerului. Starea activă trăiește în colecții thread-safe (Group); dacă jurnalul este
/// persistent, fiecare modificare importantă este scrisă și în WAL, iar la pornire starea se reconstruiește din el.
/// </summary>
public sealed class MessageStore
{
    private sealed class JournalRecord
    {
        [JsonPropertyName("op")] public string Op { get; set; } = "";
        [JsonPropertyName("topic")] public string? Topic { get; set; }
        [JsonPropertyName("group")] public string? Group { get; set; }
        [JsonPropertyName("id")] public string? Id { get; set; }
        [JsonPropertyName("d")] public Delivery? D { get; set; }
    }

    private static readonly JsonSerializerOptions JsonOpts = new() { DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull };

    private const string OpPub = "pub", OpAck = "ack", OpGrp = "grp";
    public const string DlqTopic = "$dlq";

    private readonly BrokerOptions _opt;
    private readonly TopicRegistry _registry;
    private readonly IJournal _journal;
    private readonly ILogger _log;
    private readonly ConcurrentDictionary<string, long> _seen = new();
    private long _lastPrune;

    public MessageStore(BrokerOptions opt, TopicRegistry registry, IJournal journal, ILogger log)
    {
        _opt = opt;
        _registry = registry;
        _journal = journal;
        _log = log;
    }

    public TopicRegistry Registry => _registry;

    // ---------- pornire / recuperare ----------

    public async Task RecoverAsync()
    {
        var lines = await _journal.LoadAsync().ConfigureAwait(false);
        var groups = new HashSet<(string Topic, string Group)>();
        var live = new Dictionary<string, Delivery>(); // cheie: topic\1group\1id
        int bad = 0;

        foreach (var line in lines)
        {
            JournalRecord? r;
            try { r = JsonSerializer.Deserialize<JournalRecord>(line, JsonOpts); }
            catch (JsonException) { bad++; continue; } // ex: ultima linie scrisă parțial la cădere
            if (r is null) { bad++; continue; }

            switch (r.Op)
            {
                case OpGrp when r.Topic is not null && r.Group is not null:
                    groups.Add((r.Topic, r.Group));
                    break;
                case OpPub when r.D is not null:
                    live[r.D.Topic + "\u0001" + r.D.Group + "\u0001" + r.D.Id] = r.D;
                    break;
                case OpAck when r.Topic is not null && r.Group is not null && r.Id is not null:
                    live.Remove(r.Topic + "\u0001" + r.Group + "\u0001" + r.Id);
                    break;
                default:
                    bad++;
                    break;
            }
        }

        if (!_journal.IsPersistent) { _journal.Start(); return; }

        foreach (var d in live.Values) groups.Add((d.Topic, d.Group));

        // compactare: rescriem jurnalul doar cu starea vie
        var compact = groups.Where(g => g.Group != Group.BacklogName)
            .Select(g => Serialize(new JournalRecord { Op = OpGrp, Topic = g.Topic, Group = g.Group }))
            .Concat(live.Values.Select(d => Serialize(new JournalRecord { Op = OpPub, D = d })));
        await _journal.RewriteAsync(compact).ConfigureAwait(false);
        _journal.Start();

        var now = DateTime.UtcNow;
        foreach (var (topic, name) in groups) _registry.GetOrAdd(topic, name, out _);
        foreach (var d in live.Values)
        {
            d.NextAttemptUtc = now;
            if (_registry.GetOrAdd(d.Topic, d.Group, out _).TryAdd(d))
                _seen[SeenKey(d.Topic, d.Id)] = now.Ticks;
        }

        foreach (var topic in _registry.Topics.ToList())
            if (_registry.TryGetBacklog(topic, out var b) && b!.Count > 0 && _registry.RealGroups(topic).Length > 0)
                await MoveBacklogAsync(topic).ConfigureAwait(false);

        _log.LogInformation("Recuperat din jurnal: {Groups} grupuri, {Messages} mesaje neconfirmate ({Bad} înregistrări ignorate)",
            groups.Count, live.Count, bad);
    }

    // ---------- publicare ----------

    public async Task<bool> PublishAsync(Frame f)
    {
        var topic = f.Topic!;
        var key = SeenKey(topic, f.Id!);
        if (!_seen.TryAdd(key, DateTime.UtcNow.Ticks))
        {
            Metrics.OnDuplicate();
            return true; // duplicat: confirmăm idempotent, fără a re-pune în coadă
        }

        try
        {
            var groups = _registry.TargetGroups(topic);
            foreach (var g in groups)
                if (g.Count >= _opt.MaxGroupQueue)
                    throw new ProtocolException(ErrorCodes.QueueFull, $"coada grupului '{g.Name}' este plină", f.Id);

            DateTime? expires = f.TtlMs is > 0
                ? DateTime.UtcNow.AddMilliseconds(f.TtlMs.Value)
                : _opt.DefaultTtlMs > 0 ? DateTime.UtcNow.AddMilliseconds(_opt.DefaultTtlMs) : null;

            var proto = new Delivery
            {
                Id = f.Id!,
                Topic = topic,
                Type = f.Type,
                Timestamp = f.Timestamp,
                Payload = f.Payload!.Value.GetRawText(),
                ExpiresUtc = expires
            };

            var deliveries = groups.Select(g => proto.CloneFor(g.Name)).ToList();
            // durabil întâi, apoi vizibil pentru workeri -> un ACK nu poate precede înregistrarea PUB în jurnal
            await Task.WhenAll(deliveries.Select(d => _journal.AppendAsync(Serialize(new JournalRecord { Op = OpPub, D = d }))))
                .ConfigureAwait(false);

            var now = DateTime.UtcNow;
            for (int i = 0; i < deliveries.Count; i++)
            {
                deliveries[i].NextAttemptUtc = now;
                groups[i].TryAdd(deliveries[i]);
            }

            Metrics.OnPublished();

            // s-a abonat cineva chiar în timpul publicării? atunci mutăm backlog-ul
            if (groups[0].IsBacklog && _registry.RealGroups(topic).Length > 0)
                await MoveBacklogAsync(topic).ConfigureAwait(false);
            return false;
        }
        catch (ProtocolException)
        {
            _seen.TryRemove(key, out _);
            throw;
        }
        catch (Exception e)
        {
            _seen.TryRemove(key, out _);
            _log.LogError(e, "Publicarea mesajului {Id} a eșuat", f.Id);
            throw new ProtocolException(ErrorCodes.Internal, "stocarea mesajului a eșuat", f.Id);
        }
    }

    // ---------- abonare ----------

    public async Task<Group> SubscribeAsync(ClientSession session, string topic, string groupName)
    {
        var g = _registry.GetOrAdd(topic, groupName, out var created);
        if (created)
            await _journal.AppendAsync(Serialize(new JournalRecord { Op = OpGrp, Topic = topic, Group = groupName })).ConfigureAwait(false);
        g.AddMember(session);

        if (_registry.TryGetBacklog(topic, out var backlog) && backlog!.Count > 0)
            await MoveBacklogAsync(topic).ConfigureAwait(false);
        return g;
    }

    private async Task MoveBacklogAsync(string topic)
    {
        var real = _registry.RealGroups(topic);
        if (real.Length == 0 || !_registry.TryGetBacklog(topic, out var backlog)) return;

        var target = real[0];
        var drained = backlog!.DrainAll();
        if (drained.Count == 0) return;

        var tasks = new List<Task>(drained.Count * 2);
        var moved = new List<Delivery>(drained.Count);
        foreach (var old in drained)
        {
            var copy = old.CloneFor(target.Name);
            copy.NextAttemptUtc = DateTime.UtcNow;
            tasks.Add(_journal.AppendAsync(Serialize(new JournalRecord { Op = OpPub, D = copy })));
            tasks.Add(_journal.AppendAsync(Serialize(new JournalRecord { Op = OpAck, Topic = old.Topic, Group = old.Group, Id = old.Id })));
            moved.Add(copy);
        }
        await Task.WhenAll(tasks).ConfigureAwait(false);
        foreach (var d in moved) target.TryAdd(d);
        _log.LogInformation("Backlog-ul topicului '{Topic}' ({Count} mesaje) a fost atribuit grupului '{Group}'",
            topic, moved.Count, target.Name);
    }

    // ---------- confirmări, expirări, dead letter ----------

    public bool Ack(Group g, string id)
    {
        if (!g.TryRemove(id, out var d)) return false; // ACK întârziat/duplicat: ignorat
        Metrics.OnAcked();
        _ = JournalRemoveAsync(d!);
        return true;
    }

    public Task JournalRemoveAsync(Delivery d) =>
        SafeAppendAsync(Serialize(new JournalRecord { Op = OpAck, Topic = d.Topic, Group = d.Group, Id = d.Id }));

    public async Task DropExpiredAsync(Delivery d)
    {
        Metrics.OnExpired();
        _log.LogInformation("Mesajul {Id} ({Topic}/{Group}) a expirat (TTL)", d.Id, d.Topic, d.Group);
        await JournalRemoveAsync(d).ConfigureAwait(false);
    }

    /// <summary>Mută un mesaj care a epuizat încercările în topicul "$dlq" (grupul "dlq").</summary>
    public async Task DeadLetterAsync(Delivery d, string reason)
    {
        Metrics.OnDead();
        await JournalRemoveAsync(d).ConfigureAwait(false);

        if (d.Topic == DlqTopic)
        {
            _log.LogWarning("Mesaj din DLQ abandonat: {Id}", d.Id);
            return;
        }

        _log.LogWarning("Mesajul {Id} ({Topic}/{Group}) trimis în DLQ după {Attempts} încercări: {Reason}",
            d.Id, d.Topic, d.Group, d.Attempts, reason);

        try
        {
            // DLQ-ul e un topic obișnuit ("$dlq"): se rutează la grupurile lui abonate sau în backlog
            var targets = _registry.TargetGroups(DlqTopic).Where(g => g.Count < _opt.MaxGroupQueue).ToArray();
            if (targets.Length == 0) return;

            var body = new JsonObject
            {
                ["originalId"] = d.Id,
                ["topic"] = d.Topic,
                ["group"] = d.Group,
                ["reason"] = reason,
                ["attempts"] = d.Attempts,
                ["payload"] = JsonNode.Parse(d.Payload)
            };
            var proto = new Delivery
            {
                Id = Guid.NewGuid().ToString("N"),
                Topic = DlqTopic,
                Type = "DeadLetter",
                Timestamp = DateTime.UtcNow.ToString("O"),
                Payload = body.ToJsonString()
            };
            var copies = targets.Select(g => proto.CloneFor(g.Name)).ToList();
            await Task.WhenAll(copies.Select(c => _journal.AppendAsync(Serialize(new JournalRecord { Op = OpPub, D = c }))))
                .ConfigureAwait(false);
            for (int i = 0; i < copies.Count; i++)
            {
                copies[i].NextAttemptUtc = DateTime.UtcNow;
                targets[i].TryAdd(copies[i]);
            }
            if (targets[0].IsBacklog && _registry.RealGroups(DlqTopic).Length > 0)
                await MoveBacklogAsync(DlqTopic).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            _log.LogError(e, "Nu s-a putut scrie în DLQ mesajul {Id}", d.Id);
        }
    }

    // ---------- replicare (cluster primary/standby) ----------

    /// <summary>Starea curentă ca linii de jurnal (grupuri + mesaje active), trimisă unui standby la sincronizare.</summary>
    public List<string> SnapshotLines()
    {
        var lines = new List<string>();
        foreach (var g in _registry.AllGroups)
        {
            if (!g.IsBacklog)
                lines.Add(Serialize(new JournalRecord { Op = OpGrp, Topic = g.Topic, Group = g.Name }));
            foreach (var d in g.Snapshot())
                lines.Add(Serialize(new JournalRecord { Op = OpPub, D = d }));
        }
        return lines;
    }

    /// <summary>Standby: înlocuiește complet starea locală cu snapshot-ul primit de la lider.</summary>
    public async Task ReplaceStateAsync(IReadOnlyList<string> lines)
    {
        _registry.Clear();
        _seen.Clear();
        foreach (var line in lines) ApplyToMemory(line);
        await _journal.ReplaceAsync(lines).ConfigureAwait(false);
        _log.LogInformation("Standby sincronizat: {Count} înregistrări din snapshot", lines.Count);
    }

    /// <summary>Standby: aplică o înregistrare de jurnal primită în flux de la lider.</summary>
    public async Task ApplyReplicatedAsync(string line)
    {
        if (ApplyToMemory(line))
            await _journal.AppendAsync(line).ConfigureAwait(false);
    }

    private bool ApplyToMemory(string line)
    {
        JournalRecord? r;
        try { r = JsonSerializer.Deserialize<JournalRecord>(line, JsonOpts); }
        catch (JsonException) { return false; }
        if (r is null) return false;

        switch (r.Op)
        {
            case OpGrp when r.Topic is not null && r.Group is not null:
                _registry.GetOrAdd(r.Topic, r.Group, out _);
                return true;
            case OpPub when r.D is not null:
                r.D.NextAttemptUtc = DateTime.UtcNow;
                _registry.GetOrAdd(r.D.Topic, r.D.Group, out _).TryAdd(r.D);
                _seen[SeenKey(r.D.Topic, r.D.Id)] = DateTime.UtcNow.Ticks;
                return true;
            case OpAck when r.Topic is not null && r.Group is not null && r.Id is not null:
                if (_registry.TryGet(r.Topic, r.Group, out var g)) g!.TryRemove(r.Id, out _);
                return true;
            default:
                return false;
        }
    }

    // ---------- întreținere ----------

    public void PruneSeen()
    {
        var now = DateTime.UtcNow.Ticks;
        if (now - Interlocked.Read(ref _lastPrune) < TimeSpan.FromSeconds(30).Ticks) return;
        Interlocked.Exchange(ref _lastPrune, now);
        var cutoff = now - TimeSpan.FromMilliseconds(_opt.DedupWindowMs).Ticks;
        foreach (var kv in _seen)
            if (kv.Value < cutoff) _seen.TryRemove(kv.Key, out _);
    }

    // ---------- utilitare ----------

    private async Task SafeAppendAsync(string line)
    {
        try { await _journal.AppendAsync(line).ConfigureAwait(false); }
        catch (Exception e) { _log.LogError(e, "Scrierea în jurnal a eșuat"); }
    }

    private static string Serialize(JournalRecord r) => JsonSerializer.Serialize(r, JsonOpts);
    private static string SeenKey(string topic, string id) => topic + "\u0001" + id;
}
