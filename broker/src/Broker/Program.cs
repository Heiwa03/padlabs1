using System.Runtime.InteropServices;
using Broker;
using Broker.Cluster;
using Broker.Config;
using Broker.Network;
using Broker.Routing;
using Broker.Storage;
using Broker.Workers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

Console.OutputEncoding = System.Text.Encoding.UTF8; // diacritice corecte în loguri (Windows)

var config = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: true)
    .AddEnvironmentVariables() // ex: BROKER__PORT=5001, BROKER__STORAGE=file, BROKER__CLUSTER__ENABLED=true
    .AddCommandLine(args)      // ex: --Broker:Port=5001
    .Build();

var options = config.GetSection("Broker").Get<BrokerOptions>() ?? new BrokerOptions();

using var loggerFactory = LoggerFactory.Create(b => b
    .AddSimpleConsole(o => { o.SingleLine = true; o.TimestampFormat = "HH:mm:ss "; })
    .SetMinimumLevel(LogLevel.Information));
var log = loggerFactory.CreateLogger("Broker");

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };
using var sigterm = PosixSignalRegistration.Create(PosixSignal.SIGTERM, ctx => { ctx.Cancel = true; cts.Cancel(); });

var cluster = new ClusterManager(options, log);

IJournal journal = options.Storage.Equals("file", StringComparison.OrdinalIgnoreCase)
    ? new FileJournal(options.DataDir, log)
    : new NullJournal();
if (cluster.Enabled) journal = new ReplicatingJournal(journal, cluster);

var registry = new TopicRegistry();
var store = new MessageStore(options, registry, journal, log);
var retry = new RetryPolicy(options.RetryBaseMs, options.RetryMaxMs);

await store.RecoverAsync();

var server = new TcpServer(options, store, registry, retry, log, () => cluster.IsLeader);
cluster.Attach(store, server.DropAllSessions);
var health = new HealthServer(options.HealthPrefix, () => cluster.IsLeader, log, () => cluster.RoleName);

int workerCount = Math.Max(1, options.Workers);
var tasks = new List<Task> { server.RunAsync(cts.Token), health.RunAsync(cts.Token), cluster.RunAsync(cts.Token) };
for (int i = 0; i < workerCount; i++)
    tasks.Add(new DeliveryWorker(i, workerCount, options, registry, store, retry, log, () => cluster.IsLeader).RunAsync(cts.Token));

log.LogInformation("Broker pornit: {Workers} workeri de livrare, ack timeout {Ack} ms, max {Max} încercări, cluster: {Cluster}",
    workerCount, options.AckTimeoutMs, options.MaxAttempts, cluster.Enabled ? "da" : "nu");

await Task.WhenAll(tasks);

await journal.DisposeAsync();
log.LogInformation("Broker oprit curat. {Metrics}", System.Text.Json.JsonSerializer.Serialize(Metrics.Snapshot()));
