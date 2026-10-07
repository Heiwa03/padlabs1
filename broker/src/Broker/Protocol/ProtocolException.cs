namespace Broker.Protocol;

/// <summary>Eroare de protocol cauzată de client; se transformă într-un cadru ERROR, nu închide brokerul.</summary>
public sealed class ProtocolException : Exception
{
    public string Code { get; }
    public string? FrameId { get; }

    public ProtocolException(string code, string reason, string? frameId = null) : base(reason)
    {
        Code = code;
        FrameId = frameId;
    }
}
