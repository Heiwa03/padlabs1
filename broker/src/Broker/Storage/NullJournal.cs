namespace Broker.Storage;

/// <summary>Mod transient: mesajele trăiesc doar în colecțiile din memorie.</summary>
public sealed class NullJournal : IJournal
{
    public bool IsPersistent => false;
    public Task<IReadOnlyList<string>> LoadAsync() => Task.FromResult<IReadOnlyList<string>>(Array.Empty<string>());
    public Task RewriteAsync(IEnumerable<string> lines) => Task.CompletedTask;
    public void Start() { }
    public Task AppendAsync(string line) => Task.CompletedTask;
    public Task ReplaceAsync(IReadOnlyList<string> lines) => Task.CompletedTask;
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
