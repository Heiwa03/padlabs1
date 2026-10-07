using System.Net;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Broker.Network;

/// <summary>
/// Endpoint-uri HTTP minime pentru Docker/Kubernetes: /healthz (liveness), /ready (readiness), /metrics.
/// </summary>
public sealed class HealthServer
{
    private readonly string _prefix;
    private readonly Func<bool> _isReady;
    private readonly ILogger _log;
    private readonly Func<string> _role;

    public HealthServer(string prefix, Func<bool> isReady, ILogger log, Func<string>? role = null)
    {
        _prefix = prefix;
        _isReady = isReady;
        _log = log;
        _role = role ?? (() => "leader");
    }

    public async Task RunAsync(CancellationToken ct)
    {
        using var listener = new HttpListener();
        try
        {
            listener.Prefixes.Add(_prefix);
            listener.Start();
        }
        catch (Exception e)
        {
            _log.LogWarning("Serverul de health nu a pornit ({Message}); se continuă fără el", e.Message);
            return;
        }

        _log.LogInformation("Health/metrics pe {Prefix}", _prefix);
        using var reg = ct.Register(() => { try { listener.Stop(); } catch { /* ignorat */ } });

        while (!ct.IsCancellationRequested)
        {
            HttpListenerContext ctx;
            try { ctx = await listener.GetContextAsync().ConfigureAwait(false); }
            catch { break; }

            try
            {
                var path = ctx.Request.Url?.AbsolutePath ?? "/";
                int status = 200;
                string body, contentType = "text/plain";
                switch (path)
                {
                    case "/healthz": body = "ok"; break;
                    case "/ready":
                        if (_isReady()) body = "ready";
                        else { status = 503; body = "not ready"; }
                        break;
                    case "/role": body = _role(); break;
                    case "/metrics":
                        contentType = "application/json";
                        body = JsonSerializer.Serialize(Metrics.Snapshot());
                        break;
                    default: status = 404; body = "not found"; break;
                }

                var bytes = Encoding.UTF8.GetBytes(body);
                ctx.Response.StatusCode = status;
                ctx.Response.ContentType = contentType;
                ctx.Response.ContentLength64 = bytes.Length;
                await ctx.Response.OutputStream.WriteAsync(bytes, ct).ConfigureAwait(false);
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                _log.LogDebug(e, "Eroare la health request");
            }
            finally
            {
                ctx.Response.Close();
            }
        }
    }
}
