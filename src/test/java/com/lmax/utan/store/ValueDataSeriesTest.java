package com.lmax.utan.store;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static com.lmax.utan.store.Block.new4kHeapBlock;
import static com.lmax.utan.store.BlockGenerator.generateBlockData;
import static java.lang.Math.abs;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

public class ValueDataSeriesTest
{
    private final ValueDataSeries valueDataSeries = new ValueDataSeries(1234567, 4);
    private Random random = new Random(27364L);

    @Test
    public void returnValuesWithinSingleBlock() throws Exception
    {
        final Block b = new4kHeapBlock();
        final List<Entry> entries = new ArrayList<>();
        final long startTimestamp = abs(random.nextLong());
        final long endTimestamp = generateBlockData(startTimestamp, random, b, entries);

        BlockTest.assertTimestampsAndValues(b, entries);

        valueDataSeries.load(n -> singletonList(b));

        long queryStart = startTimestamp + 10_000;
        long queryEnd = endTimestamp - 10_000;
        int[] count = { 0 };

        valueDataSeries.query(
            queryStart,
            queryEnd,
            (k, v) ->
            {
                assertThat(k).isGreaterThanOrEqualTo(queryStart);
                assertThat(k).isLessThan(queryEnd);
                count[0]++;
            });

        assertThat(count[0]).isGreaterThan(0);
    }
}