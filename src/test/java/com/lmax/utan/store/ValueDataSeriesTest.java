package com.lmax.utan.store;

import com.lmax.collection.Maps;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static com.lmax.utan.store.Block.new4kHeapBlock;
import static com.lmax.utan.store.BlockGenerator.generateBlockData;
import static java.lang.Math.abs;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

public class ValueDataSeriesTest
{
    private final ValueDataSeries valueDataSeries = new ValueDataSeries(1234567, 4);
    private Random random = new Random(27364L);

    @Test
    public void returnValuesWithinSingleBlock() throws Exception
    {
        final List<Entry> entries = new ArrayList<>();
        final Block b = new4kHeapBlock();

        final long startTimestamp = abs(random.nextLong());
        final long endTimestamp = generateBlockData(startTimestamp, random, b, entries);

        assertQuery(entries, startTimestamp, endTimestamp, singletonList(b));
    }

    @Test
    public void returnValuesSpanningTwoBlocks() throws Exception
    {
        final List<Entry> entries = new ArrayList<>();
        final Block b1 = new4kHeapBlock();
        final Block b2 = new4kHeapBlock();

        final long beginTimestamp1 = abs(random.nextLong());
        final long endTimestamp1 = generateBlockData(beginTimestamp1, random, b1, entries);
        final long endTimestamp2 = generateBlockData(endTimestamp1 + 1000, random, b1, entries);

        assertQuery(entries, beginTimestamp1, endTimestamp2, asList(b1, b2));
    }

    private void assertQuery(List<Entry> entries, long beginTimestamp, long endTimestamp, List<Block> blocks)
    {
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