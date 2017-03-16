package com.lmax.utan.store;

import com.lmax.collection.Maps;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

import static com.lmax.utan.store.Block.new4kHeapBlock;
import static com.lmax.utan.store.BlockGenerator.generateBlockData;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class ValueDataSeriesTest
{
    private final ValueDataSeries valueDataSeries = new ValueDataSeries(1234567, 4);
    private Random random = new Random(27364L);

    @Test
    public void returnValuesWithinSingleBlock() throws Exception
    {
        final List<Entry> entries = new ArrayList<>();
        final Block b = new4kHeapBlock();

        TimeSeriesSupplier supplier = new TimeSeriesSupplier(54321L);

        generateBlockData(supplier, b, entries);

        assertQuery(entries, singletonList(b));
    }

    @Test
    public void returnValuesSpanningTwoBlocks() throws Exception
    {
        final List<Entry> entries = new ArrayList<>();
        final Block b1 = new4kHeapBlock();
        final Block b2 = new4kHeapBlock();

        TimeSeriesSupplier supplier = new TimeSeriesSupplier(12345L);
        generateBlockData(supplier, b1, entries);
        generateBlockData(supplier, b2, entries);

        assertQuery(entries, asList(b1, b2));
    }

    @Test
    public void inputValuesBeyondValueDataSeriesCapacity() throws Exception
    {
        Supplier<Entry> supplier = new TimeSeriesSupplier(67890);

        while (valueDataSeries.head() < valueDataSeries.capacity() + 1)
        {
            Entry entry = supplier.get();
            valueDataSeries.append(entry.timestamp, entry.value);
        }
    }

    @Test
    public void blowUpIfDataWouldGetWrapped() throws Exception
    {
        Supplier<Entry> supplier = new TimeSeriesSupplier(67890);
        CountDownLatch latch = new CountDownLatch(1);

        Runnable r = () -> valueDataSeries.query(
            0, Long.MAX_VALUE,
            (k, v) ->
            {
                try
                {
                    latch.await();
                }
                catch (InterruptedException e)
                {
                    // No-op
                }
            });

        Thread t = new Thread(r);
        t.start();

        while (valueDataSeries.head() < 4)
        {
            Entry entry = supplier.get();
            valueDataSeries.append(entry.timestamp, entry.value);
        }

        try
        {
            for (int i = 0; i < 1_000_000; i++)
            {
                Entry entry = supplier.get();
                valueDataSeries.append(entry.timestamp, entry.value);
            }

            fail("Should have thrown exception");
        }
        catch (RuntimeException e)
        {
            // No-op
        }

        try
        {
            Entry entry = supplier.get();
            valueDataSeries.append(entry.timestamp, entry.value);
            fail("Should have thrown exception");
        }
        catch (RuntimeException e)
        {
            // No-op
        }

        latch.countDown();
        t.join();
    }

    private void assertQuery(List<Entry> entries, List<Block> blocks)
    {
        long beginTimestamp = entries.get(0).timestamp;
        long endTimestamp = entries.get(entries.size() - 1).timestamp;

        Map<Long, Double> timestampToValue = Maps.mapOf(entries, (e, m) -> m.put(e.timestamp, e.value), HashMap::new);

        valueDataSeries.load(n -> blocks);

        long queryStart = beginTimestamp + 10_000;
        long queryEnd = endTimestamp - 10_000;

        int[] count = { 0 };

        valueDataSeries.query(
            queryStart,
            queryEnd,
            (k, v) ->
            {
                assertThat(k).isGreaterThanOrEqualTo(queryStart);
                assertThat(k).isLessThan(queryEnd);
                assertThat(v).isEqualTo(timestampToValue.get(k));
                count[0]++;
            });

        assertThat(count[0]).isGreaterThan(0);
    }
}