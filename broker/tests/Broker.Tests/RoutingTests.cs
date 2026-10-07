using Broker.Routing;
using Broker.Workers;
using Xunit;

namespace Broker.Tests;

public class RoutingTests
{
    private static Delivery D(string id, DateTime? exp = null) =>
        new() { Id = id, Topic = "t", Group = "g", Payload = "1", ExpiresUtc = exp };

    [Fact]
    public void RetryPolicy_is_exponential_and_capped()
    {
        var p = new RetryPolicy(100, 1000);
        Assert.Equal(TimeSpan.FromMilliseconds(100), p.Delay(1));
        Assert.Equal(TimeSpan.FromMilliseconds(200), p.Delay(2));
        Assert.Equal(TimeSpan.FromMilliseconds(400), p.Delay(3));
        Assert.Equal(TimeSpan.FromMilliseconds(1000), p.Delay(10));
        Assert.Equal(TimeSpan.FromMilliseconds(1000), p.Delay(500));
    }

    [Fact]
    public void Group_rejects_duplicate_ids_and_removes_on_ack()
    {
        var g = new Group("t", "g");
        Assert.True(g.TryAdd(D("1")));
        Assert.False(g.TryAdd(D("1")));
        Assert.Equal(1, g.Count);
        Assert.True(g.TryRemove("1", out _));
        Assert.False(g.TryRemove("1", out _));
        Assert.Equal(0, g.Count);
    }

    [Fact]
    public void Group_without_members_keeps_messages_pending()
    {
        var g = new Group("t", "g");
        g.TryAdd(D("1"));
        var sends = new List<(Delivery, int, Broker.Network.ClientSession)>();
        var dead = new List<Delivery>();
        var expired = new List<Delivery>();
        g.Collect(DateTime.UtcNow, 1000, 3, 10, 100, new RetryPolicy(10, 100), sends, dead, expired);
        Assert.Empty(sends);
        Assert.Empty(dead);
        Assert.Equal(1, g.Count);
    }

    [Fact]
    public void Group_expires_old_messages()
    {
        var g = new Group("t", "g");
        g.TryAdd(D("old", DateTime.UtcNow.AddSeconds(-1)));
        g.TryAdd(D("fresh", DateTime.UtcNow.AddMinutes(1)));
        var sends = new List<(Delivery, int, Broker.Network.ClientSession)>();
        var dead = new List<Delivery>();
        var expired = new List<Delivery>();
        g.Collect(DateTime.UtcNow, 1000, 3, 10, 100, new RetryPolicy(10, 100), sends, dead, expired);
        Assert.Equal(new[] { "old" }, expired.Select(d => d.Id));
        Assert.Equal(1, g.Count);
    }

    [Fact]
    public void Registry_routes_to_backlog_until_a_real_group_exists()
    {
        var r = new TopicRegistry();
        var targets = r.TargetGroups("orders");
        Assert.Single(targets);
        Assert.True(targets[0].IsBacklog);

        r.GetOrAdd("orders", "workers", out var created);
        Assert.True(created);
        var real = r.TargetGroups("orders");
        Assert.Single(real);
        Assert.Equal("workers", real[0].Name);
    }
}
