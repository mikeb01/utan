package com.lmax.utan;

import com.lmax.utan.io.Dirs;
import com.lmax.utan.store.Block;
import com.lmax.utan.store.ConcurrentStore;
import com.lmax.utan.store.Cursor;
import com.lmax.utan.store.Entry;
import com.lmax.utan.store.PersistentStoreReader;
import com.lmax.utan.store.TimeSeriesSupplier;
import org.agrona.IoUtil;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class CollectorIntegrationTest
{
    @Test
    public void runProducerAndCollector() throws Exception
    {
        File dir = new File("tmp/utan");
        IoUtil.ensureDirectoryExists(dir, "test");

        final ConcurrentStore store = new ConcurrentStore(
            dir,
            (r) ->
            {
                final Thread thread = new Thread(r);
                thread.setDaemon(true);
                return thread;
            });

        store.start();

        PersistentStoreReader reader = new PersistentStoreReader(dir);

        long startTimestamp = System.currentTimeMillis();

        List<TimeseriesTestData> inputData = new ArrayList<>();
        for (int i = 0; i < 20; i++)
        {
            final String key = ThreadLocalRandom.current().ints(0x20, 0x7F).limit(20).mapToObj(String::valueOf).collect(joining());
            inputData.add(new TimeseriesTestData(key, new TimeSeriesSupplier(2, startTimestamp)));
        }

        inputData.forEach(
            td ->
            {
                for (int i = 0; i < 1000; i++)
                {
                    final Entry entry = td.next();
                    store.append(td.key, entry.timestamp, entry.value);
                }
            });


        boolean isComplete = false;
        for (int i = 0; i < 5 && !isComplete; i++)
        {
            Thread.sleep(1000);
            isComplete = isComplete(reader, inputData);
        }

        inputData.forEach(
            td ->
            {
                List<Entry> readData = new ArrayList<>();
                try (final Cursor<Block> query = reader.query(td.key, td.firstTimestamp(), td.endTimestamp() + 1))
                {
                    while (query.moveNext())
                    {
                        Block block = query.current();
                        block.foreach((timestamp, value) -> readData.add(new Entry(timestamp, value)));
                    }

                    assertThat(readData).isEqualTo(td.generatedData);
                }
                catch (IOException e)
                {
                    fail(e.getMessage());
                }
            });

        store.stopAndWait();
        Dirs.delete(dir);
    }

    private boolean isComplete(PersistentStoreReader reader, List<TimeseriesTestData> inputData)
    {
        return inputData.stream().allMatch(
            td ->
            {
                try
                {
                    try (final Cursor<Block> query = reader.query(td.key, td.endTimestamp(), td.endTimestamp() + 1))
                    {
                        return query.moveNext() && query.current().lastTimestamp() == td.endTimestamp();
                    }
                }
                catch (IOException e)
                {
                    return false;
                }
            });
    }

    @Test
    public void appendToCollector() throws Exception
    {
        File dir = Dirs.createTempDir("CollectorIntegrationTest-runProducerAndCollector");

        final ConcurrentStore store = new ConcurrentStore(
            dir,
            (r) ->
            {
                final Thread thread = new Thread(r);
                thread.setDaemon(true);
                return thread;
            });

        long startTimestamp = System.currentTimeMillis();

        List<TimeseriesTestData> inputData = new ArrayList<>();

        final String key = ThreadLocalRandom.current().ints(0x20, 0x7F).limit(20).mapToObj(String::valueOf).collect(joining());
        final TimeseriesTestData td = new TimeseriesTestData(key, new TimeSeriesSupplier(2, startTimestamp));

        for (int i = 0; i < 1000; i++)
        {
            final Entry entry = td.next();
            store.append(td.key, entry.timestamp, entry.value);
        }
    }

    private static class TimeseriesTestData
    {
        private final String key;
        private final TimeSeriesSupplier supplier;
        private final List<Entry> generatedData;

        private TimeseriesTestData(String key, TimeSeriesSupplier supplier)
        {
            this.key = key;
            this.supplier = supplier;
            this.generatedData = new ArrayList<>();
        }

        public Entry next()
        {
            final Entry entry = supplier.get();
            generatedData.add(entry);
            return entry;
        }

        public long firstTimestamp()
        {
            return generatedData.get(0).timestamp;
        }

        public long endTimestamp()
        {
            return generatedData.get(generatedData.size() - 1).timestamp;
        }
    }
}
