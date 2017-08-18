package com.lmax.utan.store;

import com.lmax.collection.Maps;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.function.Supplier;

import static com.lmax.utan.store.Block.newHeapBlock;
import static com.lmax.utan.store.BlockGenerator.generateBlockData;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class InMemoryTimeSeriesTest
{
    private final InMemoryTimeSeries inMemoryTimeSeries = new InMemoryTimeSeries(4);
    private Random random = new Random(27364L);

    @Test
    public void returnValuesWithinSingleBlock() throws Exception
    {
        final List<Entry> entries = new ArrayList<>();
        final Block b = newHeapBlock();

        TimeSeriesSupplier supplier = new TimeSeriesSupplier(54321L);

        generateBlockData(supplier, b, entries);

        assertQuery(entries, singletonList(b));
    }

    @Test
    public void returnValuesSpanningTwoBlocks() throws Exception
    {
        final List<Entry> entries = new ArrayList<>();
        final Block b1 = newHeapBlock();
        final Block b2 = newHeapBlock();

        TimeSeriesSupplier supplier = new TimeSeriesSupplier(12345L);
        generateBlockData(supplier, b1, entries);
        generateBlockData(supplier, b2, entries);

        assertQuery(entries, asList(b1, b2));
    }

    @Test
    public void inputValuesBeyondValueDataSeriesCapacity() throws Exception
    {
        Supplier<Entry> supplier = new TimeSeriesSupplier(67890);

        while (inMemoryTimeSeries.head() < inMemoryTimeSeries.capacity() + 1)
        {
            Entry entry = supplier.get();
            inMemoryTimeSeries.append(entry.timestamp, entry.value);
        }
    }

    @Test
    public void blowUpIfDataWouldGetWrapped() throws Exception
    {
        Supplier<Entry> supplier = new TimeSeriesSupplier(67890);
        CountDownLatch latch = new CountDownLatch(1);

        {
            Entry entry = supplier.get();
            inMemoryTimeSeries.append(entry.timestamp, entry.value);
        }

        Runnable r = () -> inMemoryTimeSeries.query(
            0, Long.MAX_VALUE,
            (k, v) ->
            {
                try
                {
                    latch.await();
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }

                return true;
            });

        Thread t = new Thread(r);
        t.setName("foo");
        t.start();

        while (inMemoryTimeSeries.head() < 4)
        {
            Entry entry = supplier.get();
            inMemoryTimeSeries.append(entry.timestamp, entry.value);
        }

        try
        {
            for (int i = 0; i < 1_000_000; i++)
            {
                Entry entry = supplier.get();
                inMemoryTimeSeries.append(entry.timestamp, entry.value);
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
            inMemoryTimeSeries.append(entry.timestamp, entry.value);
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

        inMemoryTimeSeries.load(n -> blocks);

        long queryStart = beginTimestamp + 10_000;
        long queryEnd = endTimestamp - 10_000;

        int[] count = { 0 };

        inMemoryTimeSeries.query(
            queryStart,
            queryEnd,
            (k, v) ->
            {
                assertThat(k).isGreaterThanOrEqualTo(queryStart);
                assertThat(k).isLessThan(queryEnd);
                assertThat(v).isEqualTo(timestampToValue.get(k));
                count[0]++;

                return true;
            });

        assertThat(count[0]).isGreaterThan(0);
    }
}