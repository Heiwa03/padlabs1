using System.Text;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace Broker.Storage;

/// <summary>
/// Jurnal persistent: fișier JSONL append-only. Un singur task scriitor, care grupează înregistrările
/// disponibile într-un lot, face un singur fsync și apoi confirmă toate apelurile din lot (group commit).
/// </summary>
public sealed class FileJournal : IJournal
{
    private sealed record Item(string? Line, IReadOnlyList<string>? Replace, TaskCompletionSource Done);

    private static readonly UTF8Encoding Utf8 = new(false);

    private readonly string _path;
    private readonly ILogger _log;
    private readonly Channel<Item> _channel =
        Channel.CreateUnbounded<Item>(new UnboundedChannelOptions { SingleReader = true });
    private Task? _writer;
    private FileStream? _file;

    public FileJournal(string dataDir, ILogger log)
    {
        Directory.CreateDirectory(dataDir);
        _path = Path.Combine(dataDir, "broker.journal");
        _log = log;
    }

    public bool IsPersistent => true;

    public async Task<IReadOnlyList<string>> LoadAsync()
    {
        if (!File.Exists(_path)) return Array.Empty<string>();
        var lines = await File.ReadAllLinesAsync(_path, Encoding.UTF8).ConfigureAwait(false);
        return lines.Where(l => l.Length > 0).ToList();
    }

    public async Task RewriteAsync(IEnumerable<string> lines)
    {
        var tmp = _path + ".tmp";
        await using (var fs = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None))
        await using (var w = new StreamWriter(fs, Utf8))
        {
            foreach (var l in lines) await w.WriteLineAsync(l).ConfigureAwait(false);
            await w.FlushAsync().ConfigureAwait(false);
            fs.Flush(true);
        }
        File.Move(tmp, _path, overwrite: true);
    }

    public void Start()
    {
        _file = OpenAppend();
        _writer = Task.Run(WriteLoopAsync);
    }

    public Task AppendAsync(string line) => Enqueue(new Item(line, null, NewTcs()));

    public Task ReplaceAsync(IReadOnlyList<string> lines) => Enqueue(new Item(null, lines, NewTcs()));

    private Task Enqueue(Item item)
    {
        if (!_channel.Writer.TryWrite(item))
            item.Done.TrySetException(new InvalidOperationException("jurnalul este închis"));
        return item.Done.Task;
    }

    private static TaskCompletionSource NewTcs() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private FileStream OpenAppend() =>
        new(_path, FileMode.Append, FileAccess.Write, FileShare.Read, 64 * 1024, FileOptions.None);

    private async Task WriteLoopAsync()
    {
        var batch = new List<Item>(256);
        var reader = _channel.Reader;
        while (await reader.WaitToReadAsync().ConfigureAwait(false))
        {
            batch.Clear();
            while (batch.Count < 1024 && reader.TryRead(out var item))
            {
                batch.Add(item);
                if (item.Replace is not null) break; // înlocuirea se execută singură, în ordine
            }
            if (batch.Count == 0) continue;

            try
            {
                if (batch[^1].Replace is { } replacement)
                {
                    await WriteBatchAsync(batch.Take(batch.Count - 1)).ConfigureAwait(false);
                    await _file!.DisposeAsync().ConfigureAwait(false);
                    await RewriteAsync(replacement).ConfigureAwait(false);
                    _file = OpenAppend();
                }
                else
                {
                    await WriteBatchAsync(batch).ConfigureAwait(false);
                }
                foreach (var item in batch) item.Done.TrySetResult();
            }
            catch (Exception e)
            {
                _log.LogError(e, "Scrierea în jurnal a eșuat");
                foreach (var item in batch) item.Done.TrySetException(e);
            }
        }
    }

    private async Task WriteBatchAsync(IEnumerable<Item> items)
    {
        var sb = new StringBuilder();
        foreach (var i in items)
            if (i.Line is not null) sb.Append(i.Line).Append('\n');
        if (sb.Length == 0) return;
        await _file!.WriteAsync(Utf8.GetBytes(sb.ToString())).ConfigureAwait(false);
        await _file.FlushAsync().ConfigureAwait(false);
        _file.Flush(true); // fsync
    }

    public async ValueTask DisposeAsync()
    {
        _channel.Writer.TryComplete();
        if (_writer is not null) await _writer.ConfigureAwait(false);
        if (_file is not null) await _file.DisposeAsync().ConfigureAwait(false);
    }
}
