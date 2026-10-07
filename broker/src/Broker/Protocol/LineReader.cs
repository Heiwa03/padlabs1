using System.Text;

namespace Broker.Protocol;

public enum ReadKind { Line, TooLarge, Eof }

public readonly record struct ReadResult(ReadKind Kind, string? Line);

/// <summary>
/// Citește linii (separate prin '\n') dintr-un stream, cu limită de dimensiune. O linie prea mare este
/// aruncată până la următorul '\n' (rezultat TooLarge), conexiunea rămâne utilizabilă.
/// </summary>
public sealed class LineReader
{
    private readonly Stream _stream;
    private readonly int _maxBytes;
    private readonly byte[] _buf = new byte[16 * 1024];
    private readonly MemoryStream _cur = new();
    private int _pos, _len;
    private bool _overflow;

    public LineReader(Stream stream, int maxBytes)
    {
        _stream = stream;
        _maxBytes = maxBytes;
    }

    public async ValueTask<ReadResult> ReadAsync(CancellationToken ct)
    {
        while (true)
        {
            if (_pos == _len)
            {
                _len = await _stream.ReadAsync(_buf.AsMemory(), ct).ConfigureAwait(false);
                _pos = 0;
                if (_len == 0)
                {
                    // EOF: o ultimă linie fără '\n' e încă validă
                    if (!_overflow && _cur.Length > 0)
                        return new ReadResult(ReadKind.Line, Take());
                    return new ReadResult(ReadKind.Eof, null);
                }
            }

            int idx = Array.IndexOf(_buf, (byte)'\n', _pos, _len - _pos);
            int end = idx < 0 ? _len : idx;
            int chunk = end - _pos;

            if (!_overflow)
            {
                if (_cur.Length + chunk > _maxBytes)
                {
                    _overflow = true;
                    _cur.SetLength(0);
                }
                else
                {
                    _cur.Write(_buf, _pos, chunk);
                }
            }

            _pos = idx < 0 ? _len : idx + 1;

            if (idx >= 0)
            {
                if (_overflow)
                {
                    _overflow = false;
                    return new ReadResult(ReadKind.TooLarge, null);
                }

                var line = Take().TrimEnd('\r');
                if (line.Length == 0) continue; // linii goale se ignoră
                return new ReadResult(ReadKind.Line, line);
            }
        }
    }

    private string Take()
    {
        var s = Encoding.UTF8.GetString(_cur.GetBuffer(), 0, (int)_cur.Length);
        _cur.SetLength(0);
        return s;
    }
}
