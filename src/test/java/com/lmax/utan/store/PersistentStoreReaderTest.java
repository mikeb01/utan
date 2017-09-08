package com.lmax.utan.store;

import com.lmax.utan.io.Dirs;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class PersistentStoreReaderTest
{
    private final String key = "asdf.afd.asdf.aghdf.";
    private final SortedMap<Long, Block> blocks = new TreeMap<>();
    private PersistentStoreWriter writer;
    private PersistentStoreReader reader;

    @Before
    public void setup() throws IOException
    {
        final File dir = Dirs.createTempDir("store");
        writer = new PersistentStoreWriter(dir);
        reader = new PersistentStoreReader(dir);

        loadData();
    }

    @Test
    public void shouldFindBlocks() throws Exception
    {
        for (int i = 0; i < 1000; i++)
        {
            final long first = blocks.firstKey();
            final long last = blocks.lastKey();

            final long startTimestamp = ThreadLocalRandom.current().nextLong(first, last);
            final long endTimestamp = ThreadLocalRandom.current().nextLong(startTimestamp + 1, last + TimeUnit.DAYS.toMillis(2));

//            assertBlockSame(startTimestamp, endTimestamp);
        }
    }

    @Test
    public void shouldFindSpecificBlocks() throws Exception
    {
        assertBlockSame(1472960849927L, 1475040629466L);
    }

    @Test
    public void shouldDetermineIfKeyExists() throws Exception
    {
        assertThat(reader.exists(key)).isTrue();
        assertThat(reader.exists("is.doesnt.exist")).isFalse();
    }

    @Test
    public void shouldGetLastTimestamp() throws Exception
    {
        assertThat(reader.lastTimestamp(key)).isEqualTo(blocks.lastKey());
    }

    private void assertBlockSame(long startTimestamp, long endTimestamp) throws IOException
    {
        final List<Block> expected = new ArrayList<>();
        blocks.tailMap(startTimestamp).forEach(
            (timestamp, block) ->
            {
                if (block.firstTimestamp() < endTimestamp)
                {
                    expected.add(block);
                }
            });

        final List<Block> actual = new ArrayList<>();
        try (final Cursor<Block> query = reader.query(key, startTimestamp, endTimestamp))
        {
            int i = 0;
            while (query.moveNext())
            {
                final Block currentBlock = query.current();

                assertThat(currentBlock.firstTimestamp())
                    .as("assertBlockSame(%dL, %dL); %s-%s", startTimestamp, endTimestamp, Block.getUtc(startTimestamp), Block.getUtc(endTimestamp))
                    .as("index: %d", i)
                    .isLessThan(endTimestamp);
                assertThat(currentBlock.lastTimestamp())
                    .as("assertBlockSame(%dL, %dL); %s-%s", startTimestamp, endTimestamp, Block.getUtc(startTimestamp), Block.getUtc(endTimestamp))
                    .as("index: %d", i)
                    .isGreaterThanOrEqualTo(startTimestamp);

                Block b = Block.newDirectBlock();
                currentBlock.copyTo(b);
                actual.add(b);
            }

            assertThat(actual)
                .as("assertBlockSame(%dL, %dL); %s-%s", startTimestamp, endTimestamp, Block.getUtc(startTimestamp), Block.getUtc(endTimestamp))
                .isEqualTo(expected);
            assertThat(actual.size()).isEqualTo(expected.size());
        }
    }

    private void loadData() throws IOException
    {
        Clock clock = Clock.systemUTC();
        ZonedDateTime dateTime = ZonedDateTime.of(2016, 9, 2, 0, 0, 0, 0, clock.getZone());

        for (int i = 0; i < 1000; i++)
        {
            final Block block = Block.newDirectBlock();

            block.append(dateTime.toInstant().toEpochMilli(), 1);
            block.append(dateTime.plusMinutes(30).toInstant().toEpochMilli(), 2);
            block.freeze();

            blocks.put(block.lastTimestamp(), block);

            writer.store(key, block);

            dateTime = dateTime.plusHours(1);
        }
    }
}