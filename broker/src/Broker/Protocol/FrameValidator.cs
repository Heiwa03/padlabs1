using System.Text.RegularExpressions;

namespace Broker.Protocol;

/// <summary>Validează structura cadrelor primite de la clienți (cap-coadă, fără efecte secundare).</summary>
public static partial class FrameValidator
{
    [GeneratedRegex(@"^[A-Za-z0-9._\-/]{1,128}$")]
    private static partial Regex NameRegex();

    [GeneratedRegex(@"^\$?[A-Za-z0-9._\-/]{1,128}$")]
    private static partial Regex TopicRegex();

    public static void Validate(Frame f)
    {
        if (string.IsNullOrEmpty(f.Action))
            throw new ProtocolException(ErrorCodes.MissingField, "câmpul 'action' lipsește", f.Id);

        switch (f.Action)
        {
            case Actions.Hello:
                Require(f.ClientId, "clientId", f);
                if (!NameRegex().IsMatch(f.ClientId!))
                    throw Invalid("clientId", f);
                if (f.Role != Roles.Publisher && f.Role != Roles.Subscriber)
                    throw new ProtocolException(ErrorCodes.InvalidField, "role trebuie să fie 'publisher' sau 'subscriber'", f.Id);
                break;

            case Actions.Publish:
                Require(f.Id, "id", f);
                if (f.Id!.Length > 128) throw Invalid("id", f);
                Require(f.Topic, "topic", f);
                if (!TopicRegex().IsMatch(f.Topic!)) throw Invalid("topic", f);
                if (f.Topic![0] == '$')
                    throw new ProtocolException(ErrorCodes.ForbiddenTopic, "topicurile care încep cu '$' sunt rezervate brokerului", f.Id);
                if (f.Payload is null)
                    throw new ProtocolException(ErrorCodes.MissingField, "câmpul 'payload' lipsește", f.Id);
                if (f.TtlMs is < 0)
                    throw Invalid("ttlMs", f);
                break;

            case Actions.Subscribe:
                Require(f.Topic, "topic", f);
                if (!TopicRegex().IsMatch(f.Topic!)) throw Invalid("topic", f);
                if (f.Group is not null && !NameRegex().IsMatch(f.Group)) throw Invalid("group", f);
                break;

            case Actions.Unsubscribe:
                Require(f.Topic, "topic", f);
                break;

            case Actions.Ack:
            case Actions.Nack:
                Require(f.Id, "id", f);
                Require(f.Topic, "topic", f);
                break;

            case Actions.Ping:
                break;

            default:
                throw new ProtocolException(ErrorCodes.UnknownAction, $"acțiune necunoscută: '{f.Action}'", f.Id);
        }
    }

    private static void Require(string? value, string name, Frame f)
    {
        if (string.IsNullOrEmpty(value))
            throw new ProtocolException(ErrorCodes.MissingField, $"câmpul '{name}' lipsește", f.Id);
    }

    private static ProtocolException Invalid(string name, Frame f) =>
        new(ErrorCodes.InvalidField, $"valoare invalidă pentru '{name}'", f.Id);
}
