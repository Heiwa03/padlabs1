namespace Broker.Protocol;

public static class ErrorCodes
{
    public const string InvalidJson = "INVALID_JSON";
    public const string MissingField = "MISSING_FIELD";
    public const string InvalidField = "INVALID_FIELD";
    public const string UnknownAction = "UNKNOWN_ACTION";
    public const string FrameTooLarge = "FRAME_TOO_LARGE";
    public const string NotHello = "NOT_HELLO";
    public const string DuplicateHello = "DUPLICATE_HELLO";
    public const string ForbiddenRole = "FORBIDDEN_ROLE";
    public const string ForbiddenTopic = "FORBIDDEN_TOPIC";
    public const string NotSubscribed = "NOT_SUBSCRIBED";
    public const string AlreadySubscribed = "ALREADY_SUBSCRIBED";
    public const string QueueFull = "QUEUE_FULL";
    public const string Timeout = "TIMEOUT";
    public const string Internal = "INTERNAL";
}

public static class Actions
{
    // client -> broker
    public const string Hello = "HELLO";
    public const string Publish = "PUBLISH";
    public const string Subscribe = "SUBSCRIBE";
    public const string Unsubscribe = "UNSUBSCRIBE";
    public const string Ack = "ACK";
    public const string Nack = "NACK";
    public const string Ping = "PING";

    // broker -> client
    public const string Welcome = "WELCOME";
    public const string Published = "PUBLISHED";
    public const string Subscribed = "SUBSCRIBED";
    public const string Unsubscribed = "UNSUBSCRIBED";
    public const string Deliver = "DELIVER";
    public const string Error = "ERROR";
    public const string Pong = "PONG";
}

public static class Roles
{
    public const string Publisher = "publisher";
    public const string Subscriber = "subscriber";
}
