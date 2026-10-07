using System.Text.Json;
using System.Text.Json.Serialization;

namespace Broker.Protocol;

public static class FrameCodec
{
    private static readonly JsonSerializerOptions Options = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = false
    };

    /// <summary>Deserializează o linie JSON. Orice problemă devine ProtocolException (INVALID_JSON).</summary>
    public static Frame Decode(string line)
    {
        try
        {
            var frame = JsonSerializer.Deserialize<Frame>(line, Options);
            if (frame is null)
                throw new ProtocolException(ErrorCodes.InvalidJson, "cadrul este null");
            return frame;
        }
        catch (JsonException e)
        {
            throw new ProtocolException(ErrorCodes.InvalidJson, e.Message);
        }
    }

    public static string Encode(Frame frame) => JsonSerializer.Serialize(frame, Options);
}
