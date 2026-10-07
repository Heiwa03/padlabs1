using Broker.Workers;
using Broker.Network;

namespace Broker.Routing;

/// <summary>
/// Un grup de abonați al unui topic. Toți membrii grupului își împart mesajele (round-robin); grupuri
/// diferite primesc fiecare câte o copie (fan-out). Reprezintă și coada (storage transient) a grupului:
/// colecții protejate de un lock propriu, deci thread-safe.
/// </summary>
public sealed class Group
{
    public const string BacklogName = "$backlog";

    private readonly object _lock = new();
    private readonly LinkedList<Delivery> _pending = new();
    private readonly Dictionary<string, Delivery> _byId = new();
    private readonly HashSet<Delivery> _inflight = new();
    private readonly List<ClientSession> _members = new();
    private int _rr;

    public Group(string topic, string name)
    {
        Topic = topic;
        Name = name;
        Key = TopicRegistry.KeyOf(topic, name);
    }

    public string Topic { get; }
    public string Name { get; }
    public string Key { get; }
    public bool IsBacklog => Name == BacklogName;

    public int Count { get { lock (_lock) return _byId.Count; } }
    public int MemberCount { get { lock (_lock) return _members.Count; } }

    public bool TryAdd(Delivery d)
    {
        lock (_lock)
        {
            if (!_byId.TryAdd(d.Id, d)) return false;
            d.State = DeliveryState.Pending;
            d.Node = _pending.AddLast(d);
            return true;
        }
    }

    /// <summary>Elimină definitiv o livrare (confirmată, expirată sau pusă în DLQ).</summary>
    public bool TryRemove(string id, out Delivery? d)
    {
        lock (_lock)
        {
            if (!_byId.Remove(id, out d)) return false;
            Unlink(d);
            return true;
        }
    }

    /// <summary>NACK: reprogramează cu backoff sau, dacă s-au epuizat încercările, o elimină (dead = true).</summary>
    public bool Nack(string id, DateTime now, RetryPolicy retry, int maxAttempts, out Delivery? d, out bool dead)
    {
        lock (_lock)
        {
            dead = false;
            if (!_byId.TryGetValue(id, out d) || d.State != DeliveryState.InFlight) return false;
            dead = Requeue(d, now, retry, maxAttempts);
            return true;
        }
    }

    public void AddMember(ClientSession s)
    {
        lock (_lock)
        {
            if (!_members.Contains(s)) _members.Add(s);
        }
    }

    /// <summary>Scoate un membru; mesajele trimise lui și neconfirmate revin imediat în coadă.</summary>
    public void RemoveMember(ClientSession s)
    {
        lock (_lock)
        {
            _members.Remove(s);
            List<Delivery>? mine = null;
            foreach (var d in _inflight)
                if (ReferenceEquals(d.Session, s)) (mine ??= new()).Add(d);
            if (mine is null) return;
            foreach (var d in mine)
            {
                _inflight.Remove(d);
                d.Session = null;
                d.State = DeliveryState.Pending;
                d.NextAttemptUtc = DateTime.UtcNow;
                d.Node = _pending.AddFirst(d);
            }
        }
    }

    /// <summary>Copie a livrărilor active (pending + in-flight), pentru sincronizarea unui standby.</summary>
    public List<Delivery> Snapshot()
    {
        lock (_lock) return _byId.Values.ToList();
    }

    /// <summary>Golește grupul (folosit la mutarea backlog-ului).</summary>
    public List<Delivery> DrainAll()
    {
        lock (_lock)
        {
            var all = _byId.Values.ToList();
            _byId.Clear();
            _pending.Clear();
            _inflight.Clear();
            foreach (var d in all) { d.Node = null; d.Session = null; }
            return all;
        }
    }

    /// <summary>
    /// Un tick al work job-ului: tratează timeout-urile de ACK, elimină mesajele expirate și alege mesajele
    /// scadente de trimis către membri (în limita ferestrei in-flight).
    /// </summary>
    public void Collect(DateTime now, int ackTimeoutMs, int maxAttempts, int window, int scanLimit, RetryPolicy retry,
        List<(Delivery D, int Attempt, ClientSession Target)> sends, List<Delivery> dead, List<Delivery> expired)
    {
        lock (_lock)
        {
            if (_inflight.Count > 0)
            {
                List<Delivery>? timedOut = null;
                foreach (var d in _inflight)
                    if ((now - d.SentUtc).TotalMilliseconds >= ackTimeoutMs)
                        (timedOut ??= new()).Add(d);
                if (timedOut is not null)
                    foreach (var d in timedOut)
                        if (Requeue(d, now, retry, maxAttempts)) dead.Add(d);
            }

            int scanned = 0;
            var node = _pending.First;
            while (node is not null && scanned < scanLimit)
            {
                var next = node.Next;
                var d = node.Value;
                scanned++;

                if (d.ExpiresUtc is { } exp && exp <= now)
                {
                    _pending.Remove(node);
                    _byId.Remove(d.Id);
                    d.Node = null;
                    expired.Add(d);
                }
                else if (_members.Count > 0 && _inflight.Count < window && d.NextAttemptUtc <= now)
                {
                    _pending.Remove(node);
                    d.Node = null;
                    _rr = (_rr + 1) % _members.Count;
                    var target = _members[_rr];
                    d.State = DeliveryState.InFlight;
                    d.SentUtc = now;
                    d.Attempts++;
                    d.Session = target;
                    _inflight.Add(d);
                    sends.Add((d, d.Attempts, target));
                }

                node = next;
            }
        }
    }

    // apelat doar sub lock
    private bool Requeue(Delivery d, DateTime now, RetryPolicy retry, int maxAttempts)
    {
        _inflight.Remove(d);
        d.Session = null;
        if (d.Attempts >= maxAttempts)
        {
            _byId.Remove(d.Id);
            return true;
        }
        d.State = DeliveryState.Pending;
        d.NextAttemptUtc = now + retry.Delay(d.Attempts);
        d.Node = _pending.AddLast(d);
        return false;
    }

    // apelat doar sub lock
    private void Unlink(Delivery d)
    {
        if (d.State == DeliveryState.InFlight) _inflight.Remove(d);
        else if (d.Node is not null && d.Node.List == _pending) _pending.Remove(d.Node);
        d.Node = null;
        d.Session = null;
    }
}
