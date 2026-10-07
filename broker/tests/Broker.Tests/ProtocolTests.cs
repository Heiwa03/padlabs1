using System.Text;
using Broker.Protocol;
using Xunit;

namespace Broker.Tests;

public class ProtocolTests
{
    private static Frame Parse(string json)
    {
        var f = FrameCodec.Decode(json);
        FrameValidator.Validate(f);
        return f;
    }

    [Theory]
    [InlineData("{nu e json", ErrorCodes.InvalidJson)]
    [InlineData("[1,2]", ErrorCodes.InvalidJson)]
    [InlineData("null", ErrorCodes.InvalidJson)]
    [InlineData("{\"action\":\"PUBLISH\",\"id\":5}", ErrorCodes.InvalidJson)]
    [InlineData("{}", ErrorCodes.MissingField)]
    [InlineData("{\"action\":\"FOO\"}", ErrorCodes.UnknownAction)]
    [InlineData("{\"action\":\"PUBLISH\",\"id\":\"1\",\"topic\":\"t\"}", ErrorCodes.MissingField)]
    [InlineData("{\"action\":\"PUBLISH\",\"id\":\"1\",\"topic\":\"$dlq\",\"payload\":1}", ErrorCodes.ForbiddenTopic)]
    [InlineData("{\"action\":\"PUBLISH\",\"id\":\"1\",\"topic\":\"a b\",\"payload\":1}", ErrorCodes.InvalidField)]
    [InlineData("{\"action\":\"HELLO\",\"clientId\":\"x\",\"role\":\"admin\"}", ErrorCodes.InvalidField)]
    [InlineData("{\"action\":\"ACK\",\"id\":\"1\"}", ErrorCodes.MissingField)]
    public void Invalid_frames_raise_protocol_exception(string json, string code)
    {
        var ex = Assert.Throws<ProtocolException>(() => Parse(json));
        Assert.Equal(code, ex.Code);
    }

    [Fact]
    public void Valid_publish_keeps_payload_untouched()
    {
        var f = Parse("{\"action\":\"PUBLISH\",\"id\":\"1\",\"topic\":\"orders\",\"payload\":{\"a\":[1,2.50,null],\"s\":\"ăț\"}}");
        Assert.Equal("{\"a\":[1,2.50,null],\"s\":\"ăț\"}", f.Payload!.Value.GetRawText());
    }

    [Fact]
    public void Dlq_topic_can_be_subscribed()
    {
        var f = Parse("{\"action\":\"SUBSCRIBE\",\"topic\":\"$dlq\"}");
        Assert.Equal("$dlq", f.Topic);
    }

    [Fact]
    public void Encode_omits_null_fields()
    {
        var s = FrameCodec.Encode(new Frame { Action = Actions.Pong });
        Assert.Equal("{\"action\":\"PONG\"}", s);
    }

    private sealed class ChunkedStream(byte[] data, int chunk) : MemoryStream(data)
    {
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default) =>
            base.ReadAsync(buffer[..Math.Min(buffer.Length, chunk)], ct);
    }

    [Fact]
    public async Task LineReader_splits_lines_across_small_reads_and_skips_empty()
    {
        var data = Encoding.UTF8.GetBytes("{\"a\":1}\r\n\n{\"b\":\"ț\"}\nultima");
        var r = new LineReader(new ChunkedStream(data, 3), 1024);
        Assert.Equal("{\"a\":1}", (await r.ReadAsync(default)).Line);
        Assert.Equal("{\"b\":\"ț\"}", (await r.ReadAsync(default)).Line);
        Assert.Equal("ultima", (await r.ReadAsync(default)).Line);
        Assert.Equal(ReadKind.Eof, (await r.ReadAsync(default)).Kind);
    }

    [Fact]
    public async Task LineReader_reports_oversized_line_and_recovers()
    {
        var data = Encoding.UTF8.GetBytes(new string('x', 100) + "\nok\n");
        var r = new LineReader(new ChunkedStream(data, 7), 50);
        Assert.Equal(ReadKind.TooLarge, (await r.ReadAsync(default)).Kind);
        var next = await r.ReadAsync(default);
        Assert.Equal(ReadKind.Line, next.Kind);
        Assert.Equal("ok", next.Line);
    }
}
