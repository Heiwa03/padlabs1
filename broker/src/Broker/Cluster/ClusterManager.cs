using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Channels;
using Broker.Config;
using Broker.Protocol;
using Broker.Storage;
using Microsoft.Extensions.Logging;

namespace Broker.Cluster;

public enum NodeRole { Starting, Standby, Leader }

/// <summary>
/// Clustering primary/standby (elimină "single point of failure" din cerință).
///
/// * Un singur nod este lider: primește clienții și livrează mesajele. Celelalte sunt standby.
/// * Liderul își replică jurnalul (aceleași înregistrări WAL) către standby prin TCP, pe portul de replicare.
/// * Un standby urmărește liderul (heartbeat). Dacă liderul dispare: nodul cu ordinalul cel mai mic (preferat)
///   se promovează imediat, celelalte după FailoverMs.
/// * Un nod care pornește caută întâi un lider existent și, dacă îl găsește, devine standby (se resincronizează).
/// * Dacă apar doi lideri (ex. partiție de rețea), cel cu ordinalul mai mare cedează (demote) și se resincronizează.
///
/// Limitări asumate: replicare asincronă (ultimele mesaje neconfirmate pot lipsi după failover) și fără cvorum
/// (nu e Raft). Detalii în docs/DOCUMENTATION.md.
/// </summary>
public sealed class ClusterManager
{
    private sealed record Peer(string Host, int Port, string Name, int Ordinal);
    private sealed class Link(TcpClient client, LineReader reader, Peer peer) : IDisposable
    {
        public LineReader Reader { get; } = reader;
        public Peer Peer { get; } = peer;
        public void Dispose() => client.Dispose();
    }

    private const string Hb = "{\"op\":\"hb\"}";
    private const string SnapBegin = "{\"op\":\"snap_begin\"}";
    private const string SnapEnd = "{\"op\":\"snap_end\"}";

    private readonly ClusterOptions _c;
    private readonly ILogger _log;
    private readonly List<Peer> _peers = new();
    private readonly object _standbyLock = new();
    private readonly List<Channel<string>> _standbys = new();
    private MessageStore? _store;
    private Action? _onDemote;
    private int _role = (int)NodeRole.Starting;

    public ClusterManager(BrokerOptions opt, ILogger log)
    {
        _c = opt.Cluster;
        _log = log;
        Ordinal = OrdinalOf(_c.NodeId);

        foreach (var raw in _c.Peers.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            // formate: "broker-1.broker:5001" (nume = prima etichetă DNS) sau "broker-1@127.0.0.1:6001" (nume explicit)
            string? name = null;
            var spec = raw;
            int at = raw.IndexOf('@');
            if (at > 0) { name = raw[..at]; spec = raw[(at + 1)..]; }
            var parts = spec.Split(':');
            var host = parts[0];
            var port = parts.Length > 1 && int.TryParse(parts[1], out var p) ? p : _c.ReplPort;
            name ??= host.Split('.')[0];
            if (name.Equals(_c.NodeId, StringComparison.OrdinalIgnoreCase)) continue; // eu însumi
            _peers.Add(new Peer(host, port, name, OrdinalOf(name)));
        }
    }

    public bool Enabled => _c.Enabled;
    public int Ordinal { get; }
    public NodeRole Role => Enabled ? (NodeRole)Volatile.Read(ref _role) : NodeRole.Leader;
    public bool IsLeader => Role == NodeRole.Leader;
    public string RoleName => Role.ToString().ToLowerInvariant();

    /// <summary>Leagă dependențele create după manager (jurnalul are nevoie de manager, managerul de store).</summary>
    public void Attach(MessageStore store, Action onDemote)
    {
        _store = store;
        _onDemote = onDemote;
    }

    /// <summary>Apelat de ReplicatingJournal pentru fiecare înregistrare scrisă de lider.</summary>
    public void Replicate(string line)
    {
        if (!Enabled || !IsLeader) return;
        lock (_standbyLock)
            foreach (var ch in _standbys)
                if (!ch.Writer.TryWrite(line)) ch.Writer.TryComplete(); // standby prea lent -> se resincronizează
    }

    public async Task RunAsync(CancellationToken ct)
    {
        if (!Enabled) return;
        _log.LogInformation("Cluster: nod {Node} (ordinal {Ordinal}), peers: {Peers}", _c.NodeId, Ordinal,
            string.Join(", ", _peers.Select(p => $"{p.Host}:{p.Port}")));
        if (_peers.Count == 0)
        {
            _log.LogWarning("Cluster activat dar fără peers -> nod singular, devine lider");
            SetRole(NodeRole.Leader);
            return;
        }
        await Task.WhenAll(ListenAsync(ct), ControlLoopAsync(ct)).ConfigureAwait(false);
    }

    // ================= bucla de control (alegerea rolului) =================

    private async Task ControlLoopAsync(CancellationToken ct)
    {
        var absentSince = DateTime.UtcNow;
        bool preferred = _peers.All(p => p.Ordinal > Ordinal);
        var needed = TimeSpan.FromMilliseconds(preferred ? _c.ProbeIntervalMs * 2 : _c.FailoverMs);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                if (IsLeader)
                {
                    await Task.Delay(_c.ProbeIntervalMs * 2, ct).ConfigureAwait(false);
                    await LeaderTickAsync(ct).ConfigureAwait(false);
                    continue;
                }

                var link = await TryConnectToLeaderAsync(ct).ConfigureAwait(false);
                if (link is not null)
                {
                    SetRole(NodeRole.Standby);
                    _log.LogInformation("Cluster: urmăresc liderul {Leader}", link.Peer.Name);
                    try { await FollowAsync(link, ct).ConfigureAwait(false); }
                    catch (Exception e) when (e is IOException or SocketException or ObjectDisposedException) { }
                    finally { link.Dispose(); }
                    if (ct.IsCancellationRequested) break;
                    _log.LogWarning("Cluster: legătura cu liderul {Leader} s-a pierdut", link.Peer.Name);
                    absentSince = DateTime.UtcNow;
                    continue;
                }

                if (DateTime.UtcNow - absentSince >= needed)
                {
                    SetRole(NodeRole.Leader);
                    continue;
                }
                await Task.Delay(_c.ProbeIntervalMs, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
            catch (Exception e)
            {
                _log.LogWarning(e, "Cluster: eroare în bucla de control");
                try { await Task.Delay(1000, ct).ConfigureAwait(false); } catch (OperationCanceledException) { break; }
            }
        }
    }

    /// <summary>Liderul verifică periodic dacă mai există un lider concurent; ordinalul mai mare cedează.</summary>
    private async Task LeaderTickAsync(CancellationToken ct)
    {
        foreach (var p in _peers)
        {
            var info = await QueryRoleAsync(p, ct).ConfigureAwait(false);
            if (info is { Role: "leader" } && p.Ordinal < Ordinal)
            {
                _log.LogWarning("Cluster: există alt lider ({Peer}, ordinal {O}) -> cedez rolul", p.Name, p.Ordinal);
                Demote();
                return;
            }
        }
    }

    private void Demote()
    {
        SetRole(NodeRole.Standby);
        _onDemote?.Invoke(); // închide sesiunile clienților
    }

    private void SetRole(NodeRole role)
    {
        var old = (NodeRole)Interlocked.Exchange(ref _role, (int)role);
        if (old != role) _log.LogWarning("Cluster: rol {Old} -> {New}", old, role);
    }

    // ================= client: conectare la lider și urmărire =================

    private async Task<Link?> TryConnectToLeaderAsync(CancellationToken ct)
    {
        foreach (var p in _peers)
        {
            TcpClient? tc = null;
            try
            {
                tc = new TcpClient { NoDelay = true };
                using (var cts = CancellationTokenSource.CreateLinkedTokenSource(ct))
                {
                    cts.CancelAfter(1000);
                    await tc.ConnectAsync(p.Host, p.Port, cts.Token).ConfigureAwait(false);
                }
                var stream = tc.GetStream();
                await SendLineAsync(stream, JsonSerializer.Serialize(new { op = "sync", node = _c.NodeId, ordinal = Ordinal, role = RoleName }), ct)
                    .ConfigureAwait(false);
                var reader = new LineReader(stream, 64 * 1024 * 1024);
                using var rcts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                rcts.CancelAfter(2000);
                var r = await reader.ReadAsync(rcts.Token).ConfigureAwait(false);
                if (r.Kind == ReadKind.Line && ParseRole(r.Line!)?.Role == "leader")
                    return new Link(tc, reader, p);
            }
            catch (Exception) when (!ct.IsCancellationRequested) { /* peer indisponibil */ }
            tc?.Dispose();
        }
        return null;
    }

    private async Task FollowAsync(Link link, CancellationToken ct)
    {
        var store = _store ?? throw new InvalidOperationException("ClusterManager nu a fost atașat");
        var snapshot = new List<string>();
        bool inSnapshot = false;

        while (true)
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(_c.FailoverMs);
            ReadResult r;
            try { r = await link.Reader.ReadAsync(cts.Token).ConfigureAwait(false); }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                _log.LogWarning("Cluster: niciun heartbeat de la lider în {Ms} ms", _c.FailoverMs);
                return;
            }
            if (r.Kind != ReadKind.Line) return;

            var line = r.Line!;
            if (line == Hb) continue;
            if (line == SnapBegin) { inSnapshot = true; snapshot.Clear(); continue; }
            if (line == SnapEnd)
            {
                await store.ReplaceStateAsync(snapshot).ConfigureAwait(false);
                inSnapshot = false;
                snapshot.Clear();
                continue;
            }
            if (inSnapshot) snapshot.Add(line);
            else await store.ApplyReplicatedAsync(line).ConfigureAwait(false);
        }
    }

    // ================= server: primește standby-uri și interogări =================

    private async Task ListenAsync(CancellationToken ct)
    {
        var listener = new TcpListener(IPAddress.Any, _c.ReplPort);
        listener.Start();
        _log.LogInformation("Cluster: port de replicare {Port}", _c.ReplPort);
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var client = await listener.AcceptTcpClientAsync(ct).ConfigureAwait(false);
                _ = Task.Run(() => HandlePeerAsync(client, ct), ct);
            }
        }
        catch (OperationCanceledException) { }
        finally { listener.Stop(); }
    }

    private async Task HandlePeerAsync(TcpClient client, CancellationToken ct)
    {
        using var _ = client;
        client.NoDelay = true;
        try
        {
            var stream = client.GetStream();
            var reader = new LineReader(stream, 64 * 1024);
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(3000);
            var first = await reader.ReadAsync(cts.Token).ConfigureAwait(false);
            if (first.Kind != ReadKind.Line) return;

            var op = JsonNode.Parse(first.Line!)?["op"]?.GetValue<string>();
            await SendLineAsync(stream, JsonSerializer.Serialize(new { op = "role", role = RoleName, node = _c.NodeId, ordinal = Ordinal }), ct)
                .ConfigureAwait(false);
            if (op != "sync" || !IsLeader) return; // "who" sau nu sunt lider: doar răspuns cu rolul

            await StreamToStandbyAsync(stream, first.Line!, ct).ConfigureAwait(false);
        }
        catch (Exception e) when (e is IOException or SocketException or ObjectDisposedException or OperationCanceledException or JsonException)
        {
            // standby-ul s-a deconectat; se va reconecta și resincroniza
        }
        catch (Exception e)
        {
            _log.LogWarning(e, "Cluster: eroare la un peer");
        }
    }

    private async Task StreamToStandbyAsync(NetworkStream stream, string hello, CancellationToken ct)
    {
        var store = _store ?? throw new InvalidOperationException("ClusterManager nu a fost atașat");
        var node = JsonNode.Parse(hello)?["node"]?.GetValue<string>() ?? "?";
        var ch = Channel.CreateBounded<string>(new BoundedChannelOptions(100_000) { SingleReader = true });
        lock (_standbyLock) _standbys.Add(ch); // întâi ne înregistrăm, apoi facem snapshot: nu se pierde nimic între ele
        _log.LogInformation("Cluster: standby {Node} conectat, trimit snapshot", node);
        try
        {
            await SendLineAsync(stream, SnapBegin, ct).ConfigureAwait(false);
            foreach (var l in store.SnapshotLines())
                await SendLineAsync(stream, l, ct).ConfigureAwait(false);
            await SendLineAsync(stream, SnapEnd, ct).ConfigureAwait(false);

            while (IsLeader && !ct.IsCancellationRequested)
            {
                string line;
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                cts.CancelAfter(_c.HeartbeatMs);
                try { line = await ch.Reader.ReadAsync(cts.Token).ConfigureAwait(false); }
                catch (OperationCanceledException) when (!ct.IsCancellationRequested) { line = Hb; }
                catch (ChannelClosedException) { break; }
                await SendLineAsync(stream, line, ct).ConfigureAwait(false);
            }
        }
        finally
        {
            lock (_standbyLock) _standbys.Remove(ch);
            ch.Writer.TryComplete();
            _log.LogInformation("Cluster: standby {Node} deconectat", node);
        }
    }

    // ================= utilitare =================

    private async Task<(string Role, int Ordinal)?> QueryRoleAsync(Peer p, CancellationToken ct)
    {
        try
        {
            using var tc = new TcpClient { NoDelay = true };
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(1000);
            await tc.ConnectAsync(p.Host, p.Port, cts.Token).ConfigureAwait(false);
            var stream = tc.GetStream();
            await SendLineAsync(stream, "{\"op\":\"who\"}", cts.Token).ConfigureAwait(false);
            var r = await new LineReader(stream, 64 * 1024).ReadAsync(cts.Token).ConfigureAwait(false);
            return r.Kind == ReadKind.Line ? ParseRole(r.Line!) : null;
        }
        catch (Exception) when (!ct.IsCancellationRequested) { return null; }
    }

    private static (string Role, int Ordinal)? ParseRole(string line)
    {
        try
        {
            var n = JsonNode.Parse(line);
            if (n?["op"]?.GetValue<string>() != "role") return null;
            return (n["role"]!.GetValue<string>(), n["ordinal"]?.GetValue<int>() ?? int.MaxValue);
        }
        catch (Exception e) when (e is JsonException or InvalidOperationException) { return null; }
    }

    private static Task SendLineAsync(NetworkStream s, string line, CancellationToken ct) =>
        s.WriteAsync(Encoding.UTF8.GetBytes(line + "\n"), ct).AsTask();

    private static int OrdinalOf(string name)
    {
        int i = name.LastIndexOf('-');
        return i >= 0 && int.TryParse(name[(i + 1)..], out var n) ? n : int.MaxValue;
    }
}
