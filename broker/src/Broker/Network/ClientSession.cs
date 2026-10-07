using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Broker.Config;
using Broker.Protocol;
using Broker.Routing;
using Broker.Storage;
using Broker.Workers;
using Microsoft.Extensions.Logging;

namespace Broker.Network;

/// <summary>
/// O conexiune de client. Fiecare sesiune rulează într-un Task propriu (thread-per-request fără a bloca canalul
/// de I/O): citirea cadrelor și scrierea răspunsurilor sunt asincrone și independente.
/// </summary>
public sealed class ClientSession
{
    private static long _nextId;

    private readonly TcpClient _tcp;
    private readonly NetworkStream _stream;
    private readonly BrokerOptions _opt;
    private readonly MessageStore _store;
    private readonly TopicRegistry _registry;
    private readonly RetryPolicy _retry;
    private readonly ILogger _log;
    private readonly Channel<string> _outbound;
    private readonly CancellationTokenSource _cts = new();
    private readonly Dictionary<string, Group> _subs = new();

    private string? _clientId;
    private string? _role;

    public ClientSession(TcpClient tcp, BrokerOptions opt, MessageStore store, TopicRegistry registry,
        RetryPolicy retry, ILogger log)
    {
        _tcp = tcp;
        _stream = tcp.GetStream();
        _opt = opt;
        _store = store;
        _registry = registry;
        _retry = retry;
        _log = log;
        Id = Interlocked.Increment(ref _nextId);
        _outbound = Channel.CreateBounded<string>(new BoundedChannelOptions(Math.Max(16, opt.OutboundQueue))
        {
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait
        });
    }

    public long Id { get; }
    public string Name => $"{_clientId ?? "anon"}#{Id}";

    /// <summary>Închide brusc conexiunea (nodul nu mai este lider, sau consumator prea lent).</summary>
    public void Abort() => _cts.Cancel();

    public IReadOnlyList<Group> SnapshotSubscriptions()
    {
        lock (_subs) return _subs.Values.ToList();
    }

    public async Task RunAsync(CancellationToken serverCt)
    {
        Metrics.ConnectionOpened();
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(serverCt, _cts.Token);
        var ct = linked.Token;
        var writer = WriteLoopAsync(serverCt);
        var remote = _tcp.Client.RemoteEndPoint;
        _log.LogInformation("Conexiune nouă {Remote} ({Id})", remote, Id);

        try
        {
            var reader = new LineReader(_stream, _opt.MaxFrameBytes);
            while (!ct.IsCancellationRequested)
            {
                int timeoutMs = _clientId is null ? _opt.HelloTimeoutMs : _opt.IdleTimeoutMs;
                using var readCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                readCts.CancelAfter(timeoutMs);

                ReadResult r;
                try
                {
                    r = await reader.ReadAsync(readCts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (!ct.IsCancellationRequested)
                {
                    SendError(ErrorCodes.Timeout, _clientId is null ? "HELLO nu a fost primit la timp" : "conexiune inactivă");
                    Close();
                    break;
                }

                if (r.Kind == ReadKind.Eof) break;
                if (r.Kind == ReadKind.TooLarge)
                {
                    Metrics.OnInvalid();
                    SendError(ErrorCodes.FrameTooLarge, $"cadru mai mare de {_opt.MaxFrameBytes} octeți");
                    continue;
                }

                await HandleLineAsync(r.Line!).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) { }
        catch (IOException) { }
        catch (SocketException) { }
        catch (ObjectDisposedException) { }
        catch (Exception e)
        {
            _log.LogError(e, "Eroare neașteptată în sesiunea {Name}", Name);
        }
        finally
        {
            _registry.RemoveSession(this);
            _outbound.Writer.TryComplete();
            try { await writer.WaitAsync(TimeSpan.FromSeconds(2)).ConfigureAwait(false); } catch { /* ignorat */ }
            _cts.Cancel();
            _tcp.Dispose();
            Metrics.ConnectionClosed();
            _log.LogInformation("Conexiune închisă {Name}", Name);
        }
    }

    // ---------- procesarea cadrelor ----------

    private async Task HandleLineAsync(string line)
    {
        Frame? frame = null;
        try
        {
            frame = FrameCodec.Decode(line);
            FrameValidator.Validate(frame);

            if (_clientId is null && frame.Action != Actions.Hello)
                throw new ProtocolException(ErrorCodes.NotHello, "primul cadru trebuie să fie HELLO", frame.Id);

            switch (frame.Action)
            {
                case Actions.Hello: OnHello(frame); break;
                case Actions.Publish: await OnPublishAsync(frame).ConfigureAwait(false); break;
                case Actions.Subscribe: await OnSubscribeAsync(frame).ConfigureAwait(false); break;
                case Actions.Unsubscribe: OnUnsubscribe(frame); break;
                case Actions.Ack: OnAck(frame); break;
                case Actions.Nack: await OnNackAsync(frame).ConfigureAwait(false); break;
                case Actions.Ping: Send(new Frame { Action = Actions.Pong }); break;
            }
        }
        catch (ProtocolException pe)
        {
            Metrics.OnInvalid();
            _log.LogWarning("{Name}: cadru respins [{Code}] {Reason}", Name, pe.Code, pe.Message);
            SendError(pe.Code, pe.Message, pe.FrameId);
            if (pe.Code == ErrorCodes.NotHello) Close();
        }
        catch (OperationCanceledException) { throw; }
        catch (Exception e)
        {
            _log.LogError(e, "{Name}: eroare internă la procesarea cadrului", Name);
            SendError(ErrorCodes.Internal, "eroare internă", frame?.Id);
        }
    }

    private void OnHello(Frame f)
    {
        if (_clientId is not null)
            throw new ProtocolException(ErrorCodes.DuplicateHello, "HELLO a fost deja trimis");
        _clientId = f.ClientId;
        _role = f.Role;
        _log.LogInformation("{Name} s-a înregistrat ca {Role}", Name, _role);
        Send(new Frame { Action = Actions.Welcome, ClientId = _clientId, Role = _role });
    }

    private async Task OnPublishAsync(Frame f)
    {
        if (_role != Roles.Publisher)
            throw new ProtocolException(ErrorCodes.ForbiddenRole, "PUBLISH este permis doar rolului 'publisher'", f.Id);
        bool duplicate = await _store.PublishAsync(f).ConfigureAwait(false);
        Send(new Frame { Action = Actions.Published, Id = f.Id, Topic = f.Topic, Duplicate = duplicate ? true : null });
    }

    private async Task OnSubscribeAsync(Frame f)
    {
        if (_role != Roles.Subscriber)
            throw new ProtocolException(ErrorCodes.ForbiddenRole, "SUBSCRIBE este permis doar rolului 'subscriber'");
        var topic = f.Topic!;
        var groupName = f.Group ?? _clientId!;

        lock (_subs)
        {
            if (_subs.TryGetValue(topic, out var existing))
            {
                if (existing.Name == groupName)
                {
                    Send(new Frame { Action = Actions.Subscribed, Topic = topic, Group = groupName });
                    return;
                }
                throw new ProtocolException(ErrorCodes.AlreadySubscribed, $"deja abonat la '{topic}' în grupul '{existing.Name}'");
            }
        }

        var g = await _store.SubscribeAsync(this, topic, groupName).ConfigureAwait(false);
        lock (_subs) _subs[topic] = g;
        Send(new Frame { Action = Actions.Subscribed, Topic = topic, Group = groupName });
    }

    private void OnUnsubscribe(Frame f)
    {
        Group? g;
        lock (_subs)
        {
            if (!_subs.Remove(f.Topic!, out g))
                throw new ProtocolException(ErrorCodes.NotSubscribed, $"nu ești abonat la '{f.Topic}'");
        }
        g.RemoveMember(this);
        Send(new Frame { Action = Actions.Unsubscribed, Topic = f.Topic });
    }

    private Group SubscriptionFor(Frame f)
    {
        lock (_subs)
        {
            if (_subs.TryGetValue(f.Topic!, out var g)) return g;
        }
        throw new ProtocolException(ErrorCodes.NotSubscribed, $"nu ești abonat la '{f.Topic}'", f.Id);
    }

    private void OnAck(Frame f) => _store.Ack(SubscriptionFor(f), f.Id!);

    private async Task OnNackAsync(Frame f)
    {
        var g = SubscriptionFor(f);
        if (g.Nack(f.Id!, DateTime.UtcNow, _retry, _opt.MaxAttempts, out var d, out var dead) && dead && d is not null)
            await _store.DeadLetterAsync(d, "respins de receiver (NACK) după numărul maxim de încercări").ConfigureAwait(false);
    }

    // ---------- trimitere ----------

    /// <summary>Trimite un mesaj livrat de un worker. Fals dacă coada de ieșire e plină (consumator prea lent).</summary>
    public bool SendDeliver(Delivery d, int attempt)
    {
        using var doc = JsonDocument.Parse(d.Payload);
        return Send(new Frame
        {
            Action = Actions.Deliver,
            Id = d.Id,
            Topic = d.Topic,
            Group = d.Group,
            Type = d.Type,
            Timestamp = d.Timestamp,
            Payload = doc.RootElement,
            Attempt = attempt
        });
    }

    private bool Send(Frame frame)
    {
        if (_outbound.Writer.TryWrite(FrameCodec.Encode(frame))) return true;
        if (!_cts.IsCancellationRequested)
        {
            _log.LogWarning("{Name}: coada de ieșire plină sau închisă, conexiunea e abandonată", Name);
            _cts.Cancel();
        }
        return false;
    }

    private void SendError(string code, string reason, string? id = null) =>
        Send(new Frame { Action = Actions.Error, Code = code, Reason = reason, Id = id });

    /// <summary>Închidere grațioasă: se golește coada de ieșire, apoi sesiunea se oprește.</summary>
    private void Close() => _outbound.Writer.TryComplete();

    private async Task WriteLoopAsync(CancellationToken ct)
    {
        try
        {
            await foreach (var line in _outbound.Reader.ReadAllAsync(ct).ConfigureAwait(false))
            {
                var bytes = Encoding.UTF8.GetBytes(line + "\n");
                await _stream.WriteAsync(bytes, ct).ConfigureAwait(false);
            }
            // coada a fost închisă explicit (Close) și golită -> oprim și citirea
            _cts.Cancel();
        }
        catch (OperationCanceledException) { }
        catch (Exception e) when (e is IOException or SocketException or ObjectDisposedException)
        {
            _cts.Cancel();
        }
    }
}
