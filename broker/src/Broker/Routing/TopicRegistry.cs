using System.Collections.Concurrent;
using Broker.Network;

namespace Broker.Routing;

/// <summary>
/// Nivelul abstract de rutare: topic -> grupuri. Subiectul mesajului (topic) este identificatorul logic al
/// canalului; publisherul nu cunoaște receiverii. Dacă un topic nu are încă niciun grup, mesajele merg în
/// grupul special "$backlog" și sunt mutate în primul grup care se abonează.
/// </summary>
public sealed class TopicRegistry
{
    private readonly ConcurrentDictionary<string, Group> _groups = new();
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, Group>> _topics = new();

    public static string KeyOf(string topic, string group) => topic + "\u0001" + group;

    public IEnumerable<Group> AllGroups => _groups.Values;

    public Group GetOrAdd(string topic, string name, out bool created)
    {
        var key = KeyOf(topic, name);
        if (_groups.TryGetValue(key, out var existing))
        {
            created = false;
            return existing;
        }

        var g = new Group(topic, name);
        var winner = _groups.GetOrAdd(key, g);
        created = ReferenceEquals(winner, g);
        if (created)
            _topics.GetOrAdd(topic, _ => new()).TryAdd(name, winner);
        return winner;
    }

    public bool TryGet(string topic, string name, out Group? group) =>
        _groups.TryGetValue(KeyOf(topic, name), out group);

    /// <summary>Grupurile reale (fără backlog) ale unui topic, sortate după nume.</summary>
    public Group[] RealGroups(string topic)
    {
        if (!_topics.TryGetValue(topic, out var map)) return Array.Empty<Group>();
        return map.Values.Where(g => !g.IsBacklog).OrderBy(g => g.Name, StringComparer.Ordinal).ToArray();
    }

    /// <summary>Destinatarii unui mesaj nou: grupurile reale sau, dacă nu există, backlog-ul topicului.</summary>
    public Group[] TargetGroups(string topic)
    {
        var real = RealGroups(topic);
        if (real.Length > 0) return real;
        return new[] { GetOrAdd(topic, Group.BacklogName, out _) };
    }

    public bool TryGetBacklog(string topic, out Group? backlog) => TryGet(topic, Group.BacklogName, out backlog);

    public IEnumerable<string> Topics => _topics.Keys;

    /// <summary>Șterge toată starea (un standby care se resincronizează cu liderul).</summary>
    public void Clear()
    {
        _groups.Clear();
        _topics.Clear();
    }

    public void RemoveSession(ClientSession session)
    {
        foreach (var g in session.SnapshotSubscriptions())
            g.RemoveMember(session);
    }
}
