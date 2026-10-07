namespace Broker.Workers;

/// <summary>Backoff exponențial: base * 2^(attempt-1), plafonat la max.</summary>
public sealed class RetryPolicy
{
    private readonly int _baseMs;
    private readonly int _maxMs;

    public RetryPolicy(int baseMs, int maxMs)
    {
        _baseMs = Math.Max(1, baseMs);
        _maxMs = Math.Max(_baseMs, maxMs);
    }

    public TimeSpan Delay(int attempt)
    {
        int exp = Math.Clamp(attempt - 1, 0, 20);
        double ms = Math.Min((double)_baseMs * (1L << exp), _maxMs);
        return TimeSpan.FromMilliseconds(ms);
    }
}
