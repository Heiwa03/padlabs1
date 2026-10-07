using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Broker.Config;
using Broker.Routing;
using Broker.Storage;
using Broker.Workers;
using Microsoft.Extensions.Logging;

namespace Broker.Network;

/// <summary>Acceptă conexiuni TCP și pornește câte o sesiune (Task) pentru fiecare.</summary>
public sealed class TcpServer
{
    private readonly BrokerOptions _opt;
    private readonly MessageStore _store;
    private readonly TopicRegistry _registry;
    private readonly RetryPolicy _retry;
    private readonly ILogger _log;
    private readonly Func<bool> _acceptClients;
    private readonly ConcurrentDictionary<long, Task> _running = new();
    private readonly ConcurrentDictionary<long, ClientSession> _sessions = new();

    public TcpServer(BrokerOptions opt, MessageStore store, TopicRegistry registry, RetryPolicy retry, ILogger log,
        Func<bool>? acceptClients = null)
    {
        _opt = opt;
        _store = store;
        _registry = registry;
        _retry = retry;
        _log = log;
        _acceptClients = acceptClients ?? (() => true);
    }

    /// <summary>Închide toate sesiunile (nodul și-a pierdut rolul de lider).</summary>
    public void DropAllSessions()
    {
        foreach (var s in _sessions.Values) s.Abort();
    }

    public async Task RunAsync(CancellationToken ct)
    {
        var ip = IPAddress.TryParse(_opt.Host, out var parsed) ? parsed : IPAddress.Any;
        var listener = new TcpListener(ip, _opt.Port);
        listener.Start(512);
        _log.LogInformation("Broker TCP ascultă pe {Ip}:{Port} (storage: {Storage})", ip, _opt.Port, _opt.Storage);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                TcpClient client = await listener.AcceptTcpClientAsync(ct).ConfigureAwait(false);
                client.NoDelay = true;

                if (!_acceptClients())
                {
                    client.Dispose(); // nod standby: clienții trebuie să meargă la lider
                    continue;
                }

                if (_running.Count >= _opt.MaxConnections)
                {
                    _log.LogWarning("Număr maxim de conexiuni atins, conexiune refuzată");
                    client.Dispose();
                    continue;
                }

                var session = new ClientSession(client, _opt, _store, _registry, _retry, _log);
                _sessions[session.Id] = session;
                _running[session.Id] = Task.Run(async () =>
                {
                    try { await session.RunAsync(ct).ConfigureAwait(false); }
                    finally
                    {
                        _running.TryRemove(session.Id, out _);
                        _sessions.TryRemove(session.Id, out _);
                    }
                });
            }
        }
        catch (OperationCanceledException) { }
        finally
        {
            listener.Stop();
            _log.LogInformation("Server oprit, se așteaptă închiderea a {Count} sesiuni…", _running.Count);
            try { await Task.WhenAll(_running.Values).WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false); }
            catch { /* timeout la oprire */ }
        }
    }
}
