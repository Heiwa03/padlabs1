namespace Broker.Config;

/// <summary>Setările brokerului (appsettings.json, secțiunea "Broker", sau variabile de mediu BROKER__*).</summary>
public sealed class BrokerOptions
{
    public string Host { get; set; } = "0.0.0.0";
    public int Port { get; set; } = 5000;

    public int MaxFrameBytes { get; set; } = 1_048_576;
    public int MaxConnections { get; set; } = 1000;
    public int HelloTimeoutMs { get; set; } = 5000;
    public int IdleTimeoutMs { get; set; } = 60_000;
    public int OutboundQueue { get; set; } = 1000;

    /// <summary>Număr de work joburi (cron) care livrează mesajele din storage.</summary>
    public int Workers { get; set; } = 4;
    public int WorkerIntervalMs { get; set; } = 20;

    public int AckTimeoutMs { get; set; } = 5000;
    public int MaxAttempts { get; set; } = 5;
    public int RetryBaseMs { get; set; } = 500;
    public int RetryMaxMs { get; set; } = 30_000;
    public int InFlightWindow { get; set; } = 1000;
    public int ScanLimit { get; set; } = 2000;
    public int MaxGroupQueue { get; set; } = 100_000;
    public long DefaultTtlMs { get; set; }
    public int DedupWindowMs { get; set; } = 600_000;

    /// <summary>"memory" (transient) sau "file" (persistent, jurnal WAL).</summary>
    public string Storage { get; set; } = "memory";
    public string DataDir { get; set; } = "./data";

    public string HealthPrefix { get; set; } = "http://localhost:8080/";

    public ClusterOptions Cluster { get; set; } = new();
}

/// <summary>Clustering primary/standby (BROKER__CLUSTER__*). Dezactivat implicit.</summary>
public sealed class ClusterOptions
{
    public bool Enabled { get; set; }

    /// <summary>Numele nodului; în Kubernetes = numele pod-ului (ex. broker-0). Ordinalul = sufixul numeric.</summary>
    public string NodeId { get; set; } = Environment.GetEnvironmentVariable("HOSTNAME") ?? "broker-0";

    public int ReplPort { get; set; } = 5001;

    /// <summary>Lista tuturor nodurilor (inclusiv acesta), separate prin virgulă: "host:port,host:port".</summary>
    public string Peers { get; set; } = "";

    public int HeartbeatMs { get; set; } = 500;
    public int ProbeIntervalMs { get; set; } = 500;

    /// <summary>Cât timp lipsește liderul înainte ca un standby (non-preferat) să se promoveze.</summary>
    public int FailoverMs { get; set; } = 3000;
}
