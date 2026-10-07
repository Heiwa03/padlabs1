namespace Broker;

/// <summary>Contoare simple, thread-safe, expuse la /metrics și în loguri.</summary>
public static class Metrics
{
    private static long _connections, _published, _delivered, _retried, _acked, _dead, _expired, _invalid, _duplicates;

    public static long Connections => Interlocked.Read(ref _connections);
    public static long Published => Interlocked.Read(ref _published);
    public static long Delivered => Interlocked.Read(ref _delivered);
    public static long Retried => Interlocked.Read(ref _retried);
    public static long Acked => Interlocked.Read(ref _acked);
    public static long Dead => Interlocked.Read(ref _dead);
    public static long Expired => Interlocked.Read(ref _expired);
    public static long Invalid => Interlocked.Read(ref _invalid);
    public static long Duplicates => Interlocked.Read(ref _duplicates);

    public static void ConnectionOpened() => Interlocked.Increment(ref _connections);
    public static void ConnectionClosed() => Interlocked.Decrement(ref _connections);
    public static void OnPublished() => Interlocked.Increment(ref _published);
    public static void OnDelivered() => Interlocked.Increment(ref _delivered);
    public static void OnRetried() => Interlocked.Increment(ref _retried);
    public static void OnAcked() => Interlocked.Increment(ref _acked);
    public static void OnDead() => Interlocked.Increment(ref _dead);
    public static void OnExpired() => Interlocked.Increment(ref _expired);
    public static void OnInvalid() => Interlocked.Increment(ref _invalid);
    public static void OnDuplicate() => Interlocked.Increment(ref _duplicates);

    public static object Snapshot() => new
    {
        connections = Connections,
        published = Published,
        delivered = Delivered,
        retried = Retried,
        acked = Acked,
        deadLettered = Dead,
        expired = Expired,
        invalidFrames = Invalid,
        duplicates = Duplicates
    };
}
