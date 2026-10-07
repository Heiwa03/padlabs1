using System.Text.Json.Serialization;
using Broker.Network;

namespace Broker.Routing;

public enum DeliveryState { Pending, InFlight }

/// <summary>
/// O livrare = un mesaj destinat unui grup de abonați (topic + group). Câmpurile marcate cu JsonPropertyName
/// sunt scrise în jurnalul persistent; restul sunt stare de execuție.
/// </summary>
public sealed class Delivery
{
    [JsonPropertyName("id")] public string Id { get; set; } = "";
    [JsonPropertyName("topic")] public string Topic { get; set; } = "";
    [JsonPropertyName("group")] public string Group { get; set; } = "";
    [JsonPropertyName("type")] public string? Type { get; set; }
    [JsonPropertyName("ts")] public string? Timestamp { get; set; }
    /// <summary>Textul JSON brut al payload-ului, exact cum l-a trimis sender-ul.</summary>
    [JsonPropertyName("payload")] public string Payload { get; set; } = "null";
    [JsonPropertyName("exp")] public DateTime? ExpiresUtc { get; set; }

    [JsonIgnore] public int Attempts { get; set; }
    [JsonIgnore] public DeliveryState State { get; set; }
    [JsonIgnore] public DateTime NextAttemptUtc { get; set; }
    [JsonIgnore] public DateTime SentUtc { get; set; }
    [JsonIgnore] public ClientSession? Session { get; set; }
    [JsonIgnore] public LinkedListNode<Delivery>? Node { get; set; }

    public Delivery CloneFor(string group) => new()
    {
        Id = Id,
        Topic = Topic,
        Group = group,
        Type = Type,
        Timestamp = Timestamp,
        Payload = Payload,
        ExpiresUtc = ExpiresUtc
    };
}
