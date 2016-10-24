package com.lmax.utan.parser;

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class LineParserTest
{
    private final StubCallback callback = new StubCallback();
    private LineParser parser;

    @Before
    public void setup()
    {
        parser = new LineParser(callback);
    }

    @Test
    public void shouldParseWithNewLine() throws Exception
    {
        parse("foo.bar.gc.stopped 1001 1320764369238\r\n");
        expectGcStoppedMessage();
    }

    @Test
    public void shouldParseWithoutNewLine() throws Exception
    {
        parse("foo.bar.gc.stopped 1001 1320764369238");
        expectGcStoppedMessage();
    }

    @Test
    public void shouldParseWithUnderscore() throws Exception
    {
        parse("foo_bar.gc.stopped 1001 1320764369238");
        expectCallback("foo_bar.gc.stopped", "1001", 1320764369238L);
    }

    @Test
    public void shouldRecoverFromInvalidData() throws Exception
    {
        callback.ignoreFailure = true;

        parse("foo.bar.gc.stopped\0");
        parse("foo.bar.gc.stopped 1001 1320764369238");

        expectGcStoppedMessage();
    }

    @Test
    public void shouldParseWithoutTextualValue() throws Exception
    {
        parse("foo.bar.gc.stopped ababa 1320764369238");
        expectCallback("foo.bar.gc.stopped", "ababa", 1320764369238L);
    }

    @Test
    public void shouldParseWithTags() throws Exception
    {
        parse("foo.bar.gc.stopped 1001 1320764369238 type=ms scale=64bit");
        expectCallback("foo.bar.gc.stopped", "1001", 1320764369238L, "type", "ms", "scale", "64bit");
    }

    @Test
    public void shouldParseKeysWithHyphens() throws Exception
    {
        parse("foo.bar.a-b-c 10 1320764369238 type=counter");
        expectCallback("foo.bar.a-b-c", "10", 1320764369238L, "type", "counter");
    }

    @Test
    public void shouldParseNegativeNumbers() throws Exception
    {
        parse("foo.bar.a.b.c -10 1320764369238 type=counter");
        expectCallback("foo.bar.a.b.c", "-10", 1320764369238L, "type", "counter");
    }

    @Test
    public void shouldFailToParseKeysWithInvalidCharacters() throws Exception
    {
        assertParseException(3, "foo*bar.gc.stopped 1001 1 type=status");
    }

    @Test
    public void shouldParseMultipleLinesWithTags() throws Exception
    {
        parse("foo.bar.gc.stopped 1001 1320764369238 type=ms\nfoo.baz.gc.stopped 1002 1320764369239");
        expectCallback(0, "foo.bar.gc.stopped", "1001", 1320764369238L, "type", "ms");
        expectCallback(1, "foo.baz.gc.stopped", "1002", 1320764369239L);

        assertThat(callback.lines.size()).isEqualTo(2);
    }

    @Test
    public void shouldParseWithAdditionalWhitespace() throws Exception
    {
        parse("foo.bar.gc.stopped \t 1001\t 1320764369238   type=ms\tscale=64bit");
        expectCallback("foo.bar.gc.stopped", "1001", 1320764369238L, "type", "ms", "scale", "64bit");
    }

    @Test
    public void shouldParseMultipleLines() throws Exception
    {
        parse("foo.baz.gc.stopped 1002 1320764369237\r\nfoo.bar.gc.stopped 1001 1320764369238");
        expectCallback(0, "foo.baz.gc.stopped", "1002", 1320764369237L);
        expectCallback(1, "foo.bar.gc.stopped", "1001", 1320764369238L);

        assertThat(callback.lines.size()).isEqualTo(2);
    }

    @Test
    public void shouldFailWithNonNumericTimestamp() throws Exception
    {
        assertParseException(29, "foo.bar.gc.stopped 1001 13207a4369238 type=ms scale=64bit");
    }

    @Test
    public void shouldFailWithInvalidTag() throws Exception
    {
        assertParseException(41, "foo.bar.gc.stopped 1001 1320714369238 typ e=ms scale=64bit");
        assertParseException(42, "foo.bar.gc.stopped 1001 1320714369238 type");
        assertParseException(43, "foo.bar.gc.stopped 1001 1320714369238 type=");
    }

    @Test
    public void shouldFailMissingTimestamp() throws Exception
    {
        assertParseException(24, "foo.bar.gc.stopped 1001 ");
        assertParseException(23, "foo.bar.gc.stopped 1001");
    }

    @Test
    public void shouldFailMissingValue() throws Exception
    {
        assertParseException(19, "foo.bar.gc.stopped ");
        assertParseException(18, "foo.bar.gc.stopped");
    }

    private void assertParseException(final int location, final String line) throws IOException
    {
        try
        {
            parse(line);
            fail("Exception expected");
        }
        catch (RuntimeException e)
        {
            assertThat(e.getMessage()).endsWith("/" + location);
        }
    }

    private void parse(final String lines) throws IOException
    {
        parser.parse(new ByteArrayInputStream(lines.getBytes(UTF_8)));
    }

    private void expectGcStoppedMessage()
    {
        expectCallback("foo.bar.gc.stopped", "1001", 1320764369238L);
    }

    private void expectCallback(final String key, final String value, final long timestamp, final String... tags)
    {
        expectCallback(0, key, value, timestamp, tags);
    }

    private void expectCallback(int index, String key, String value, long timestamp, String... tags)
    {
        StubLine stubLine = callback.lines.get(index);
        assertThat(stubLine.key).isEqualTo(key);
        assertThat(stubLine.value).isEqualTo(value);
        assertThat(stubLine.timestamp).isEqualTo(timestamp);

        for (int i = 0; i < tags.length; i += 2)
        {
            assertThat(stubLine.attributes.get(tags[i])).isEqualTo(tags[i + 1]);
        }
    }

    private static class StubLine
    {
        private final long timestamp;
        private final String key;
        private final String value;
        private final Map<String, String> attributes;

        public StubLine(
            final long timestamp,
            final String key,
            final String value,
            final Map<String, String> attributes
        )
        {
            this.timestamp = timestamp;
            this.key = key;
            this.value = value;
            this.attributes = attributes;
        }
    }

    private static class StubCallback implements LineParserCallback
    {
        private final List<StubLine> lines = new ArrayList<>();
        private transient Map<String, String> attributes = new HashMap<>();
        private transient String key;
        private transient long timestamp;
        private transient String tagName;
        private transient String value;
        private boolean ignoreFailure = false;

        private void reset()
        {
            attributes = new HashMap<>();
            timestamp = 0;
            key = null;
            value = null;
            tagName = null;
        }

        @Override
        public void onKey(CharSequence key)
        {
            this.key = key.toString();
        }

        @Override
        public void onValue(CharSequence value)
        {
            this.value = value.toString();
        }

        @Override
        public void onTimestamp(long timestamp)
        {
            this.timestamp = timestamp;
        }

        @Override
        public void onTagName(CharSequence name)
        {
            this.tagName = name.toString();
        }

        @Override
        public void onTagValue(CharSequence value)
        {
            attributes.put(tagName, value.toString());
        }

        @Override
        public void onComplete()
        {
            lines.add(new StubLine(timestamp, key, value, attributes));
            reset();
        }

        @Override
        public void onFailure(String string, int location)
        {
            if (ignoreFailure)
            {
                return;
            }

            throw new RuntimeException(string + "/" + location);
        }
    }
}
