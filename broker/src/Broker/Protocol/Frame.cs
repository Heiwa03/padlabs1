using System.Text.Json;
using System.Text.Json.Serialization;

namespace Broker.Protocol;

/// <summary>
/// Un cadru al protocolului = un obiect JSON pe o linie (NDJSON). Câmpul "payload" este opac:
/// brokerul îl stochează și îl retransmite fără să-l modifice.
/// </summary>
public sealed class Frame
{
    [JsonPropertyName("action")] public string? Action { get; set; }
    [JsonPropertyName("id")] public string? Id { get; set; }
    [JsonPropertyName("topic")] public string? Topic { get; set; }
    [JsonPropertyName("group")] public string? Group { get; set; }
    [JsonPropertyName("type")] public string? Type { get; set; }
    [JsonPropertyName("timestamp")] public string? Timestamp { get; set; }
    [JsonPropertyName("payload")] public JsonElement? Payload { get; set; }
    [JsonPropertyName("ttlMs")] public long? TtlMs { get; set; }
    [JsonPropertyName("attempt")] public int? Attempt { get; set; }
    [JsonPropertyName("clientId")] public string? ClientId { get; set; }
    [JsonPropertyName("role")] public string? Role { get; set; }
    [JsonPropertyName("code")] public string? Code { get; set; }
    [JsonPropertyName("reason")] public string? Reason { get; set; }
    [JsonPropertyName("duplicate")] public bool? Duplicate { get; set; }
}
