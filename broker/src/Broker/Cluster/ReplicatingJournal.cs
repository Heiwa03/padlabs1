using Broker.Storage;

namespace Broker.Cluster;

/// <summary>
/// Decorator peste jurnalul local: fiecare înregistrare durabilă este trimisă și standby-urilor conectate
/// (replicare asincronă). Funcționează și cu NullJournal (replicare doar în memorie).
/// </summary>
public sealed class ReplicatingJournal : IJournal
{
    private readonly IJournal _inner;
    private readonly ClusterManager _cluster;

    public ReplicatingJournal(IJournal inner, ClusterManager cluster)
    {
        _inner = inner;
        _cluster = cluster;
    }

    public bool IsPersistent => _inner.IsPersistent;
    public Task<IReadOnlyList<string>> LoadAsync() => _inner.LoadAsync();
    public Task RewriteAsync(IEnumerable<string> lines) => _inner.RewriteAsync(lines);
    public void Start() => _inner.Start();
    public Task ReplaceAsync(IReadOnlyList<string> lines) => _inner.ReplaceAsync(lines);

    public Task AppendAsync(string line)
    {
        var t = _inner.AppendAsync(line);
        _cluster.Replicate(line);
        return t;
    }

    public ValueTask DisposeAsync() => _inner.DisposeAsync();
}
