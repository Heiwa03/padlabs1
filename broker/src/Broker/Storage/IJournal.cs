namespace Broker.Storage;

/// <summary>
/// Jurnal (WAL) al mesajelor. Implementarea "persistent" scrie pe disc; cea "transient" nu face nimic.
/// Apelurile Append returnează după ce înregistrarea este durabilă (fsync).
/// </summary>
public interface IJournal : IAsyncDisposable
{
    bool IsPersistent { get; }

    /// <summary>Citește toate înregistrările existente (înainte de Start).</summary>
    Task<IReadOnlyList<string>> LoadAsync();

    /// <summary>Rescrie atomic jurnalul cu starea curată (compactare la pornire).</summary>
    Task RewriteAsync(IEnumerable<string> lines);

    /// <summary>Pornește scriitorul asincron.</summary>
    void Start();

    Task AppendAsync(string line);

    /// <summary>Înlocuiește tot conținutul jurnalului cu liniile date (resincronizarea unui standby).</summary>
    Task ReplaceAsync(IReadOnlyList<string> lines);
}
