using Broker.Config;
using Broker.Network;
using Broker.Routing;
using Broker.Storage;
using Microsoft.Extensions.Logging;

namespace Broker.Workers;

/// <summary>
/// Work job (cron job): la fiecare interval parcurge grupurile care îi aparțin (partiționare după hash-ul
/// grupului, deci fără concurență între workeri pe același grup) și livrează mesajele scadente.
/// </summary>
public sealed class DeliveryWorker
{
    private readonly int _index, _count;
    private readonly BrokerOptions _opt;
    private readonly TopicRegistry _registry;
    private readonly MessageStore _store;
    private readonly RetryPolicy _retry;
    private readonly ILogger _log;
    private readonly Func<bool> _isActive;

    private readonly List<(Delivery D, int Attempt, ClientSession Target)> _sends = new();
    private readonly List<Delivery> _dead = new();
    private readonly List<Delivery> _expired = new();

    public DeliveryWorker(int index, int count, BrokerOptions opt, TopicRegistry registry, MessageStore store,
        RetryPolicy retry, ILogger log, Func<bool>? isActive = null)
    {
        _isActive = isActive ?? (() => true);
        _index = index;
        _count = count;
        _opt = opt;
        _registry = registry;
        _store = store;
        _retry = retry;
        _log = log;
    }

    public async Task RunAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(Math.Max(5, _opt.WorkerIntervalMs)));
        try
        {
            while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
            {
                try { await TickAsync().ConfigureAwait(false); }
                catch (Exception e) when (e is not OperationCanceledException)
                {
                    _log.LogError(e, "Worker {Index}: eroare în tick", _index);
                }
            }
        }
        catch (OperationCanceledException) { }
    }

    private async Task TickAsync()
    {
        if (!_isActive()) return; // standby: nu livrează, doar urmărește liderul
        var now = DateTime.UtcNow;
        foreach (var g in _registry.AllGroups)
        {
            if ((uint)g.Key.GetHashCode() % (uint)_count != (uint)_index) continue;

            _sends.Clear(); _dead.Clear(); _expired.Clear();
            g.Collect(now, _opt.AckTimeoutMs, _opt.MaxAttempts, _opt.InFlightWindow, _opt.ScanLimit, _retry,
                _sends, _dead, _expired);

            foreach (var (d, attempt, target) in _sends)
            {
                if (attempt > 1) Metrics.OnRetried();
                if (target.SendDeliver(d, attempt)) Metrics.OnDelivered();
            }
            foreach (var d in _dead) await _store.DeadLetterAsync(d, "numărul maxim de încercări depășit").ConfigureAwait(false);
            foreach (var d in _expired) await _store.DropExpiredAsync(d).ConfigureAwait(false);
        }

        if (_index == 0) _store.PruneSeen();
    }
}
