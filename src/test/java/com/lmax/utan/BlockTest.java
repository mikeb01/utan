package com.lmax.utan;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static com.lmax.utan.Block.compressBits;
import static com.lmax.utan.Block.decompressBits;
import static org.assertj.core.api.Assertions.assertThat;

public class BlockTest
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[4096]);
    private final Block b = new Block(buffer);

    @Test
    public void storeAndGetFirstValue() throws Exception
    {
        long timestamp = System.currentTimeMillis();
        double val = 78432.3;

        b.append(timestamp, val);

        final long[] timestamps = { 0 };
        final double[] values = { 0.0 };

        int count = b.foreach((t, v) ->
        {
            timestamps[0] = t;
            values[0] = v;
        });

        assertThat(count).isEqualTo(1);
        assertThat(timestamps[0]).isEqualTo(timestamp);
        assertThat(values[0]).isEqualTo(val);
    }

    @Test
    public void storeAndGetSubsequentDoubleDeltaOfZero() throws Exception
    {
        long timestamp = System.currentTimeMillis();
        double val = 78432.3;

        b.append(timestamp, val);
        b.append(timestamp, val);

        final List<Long> timestamps = new ArrayList<>();
        final List<Double> values = new ArrayList<>();

        int count = b.foreach((t, v) ->
        {
            timestamps.add(t);
            values.add(v);
        });

        assertThat(count).isEqualTo(2);
        assertThat(timestamps.get(0)).isEqualTo(timestamp);
        assertThat(values.get(0)).isEqualTo(val);
        assertThat(timestamps.get(1)).isEqualTo(timestamp);
        assertThat(values.get(1)).isEqualTo(val);
    }

    @Test
    public void storeAndGetSubsequentDoubleDeltaOfOneMillisecond() throws Exception
    {
        long timestamp = System.currentTimeMillis();
        double val = 78432.3;

        b.append(timestamp, val);
        b.append(timestamp + 1000, val + 1);
        b.append(timestamp + 2000, val - 1);

        final List<Long> timestamps = new ArrayList<>();
        final List<Double> values = new ArrayList<>();

        int count = b.foreach((t, v) ->
        {
            timestamps.add(t);
            values.add(v);
        });

        assertThat(count).isEqualTo(3);
        assertThat(timestamps.get(0)).isEqualTo(timestamp);
        assertThat(values.get(0)).isEqualTo(val);
        assertThat(timestamps.get(1)).isEqualTo(timestamp + 1000);
        assertThat(values.get(1)).isEqualTo(val + 1);
        assertThat(timestamps.get(2)).isEqualTo(timestamp + 2000);
        assertThat(values.get(2)).isEqualTo(val - 1);
    }

    @Test
    public void compressTo7Bits()
    {
        for (int i = -64, j = 64; i < 0; i++, j++)
        {
            assertThat(compressBits(i, 7)).isEqualTo(j);
            assertThat(decompressBits(compressBits(i, 7), 7)).isEqualTo(i);
        }

        for (int i = 0, j = 0; i < 64; i++, j++)
        {
            assertThat(compressBits(i, 7)).isEqualTo(j);
            assertThat(decompressBits(compressBits(i, 7), 7)).isEqualTo(i);
        }
    }

    @Test
    public void zeros() throws Exception
    {
        assertThat(Long.numberOfLeadingZeros(0)).isEqualTo(64);
        assertThat(Long.numberOfTrailingZeros(0)).isEqualTo(64);
    }

    @Test
    public void shouldInsert1000DataPoints() throws Exception
    {
        int count = 500;
        long[] timestamps = new long[count];
        double[] values = new double[count];

        Random r = new Random(7);

        long lastTimestamp = r.nextLong();
        double lastValue = r.nextDouble() * 100;

        for (int i = 0; i < count; i++)
        {
            timestamps[i] = lastTimestamp + r.nextInt(21) - 10;
            values[i] = lastValue + (10 * (r.nextDouble() - 0.5));

            lastTimestamp = timestamps[i];
            lastValue = values[i];

        }

        assertWriteAndReadValues(timestamps, values);
    }

    @Test
    public void shouldHandleLongXorValues() throws Exception
    {
        long l = 0b10000000_00000000_00000000_00000000_00000000_00000000_00000000_00000001L;

        double d = Double.longBitsToDouble(l);

        long[] timestamps = { 1000, 2000 };
        double[] values = { 0, d };

        assertWriteAndReadValues(timestamps, values);
    }

    @Test
    public void shouldWriteUntilBufferIsFull() throws Exception
    {
        Random r = new Random(7);

        List<Entry> entries = new ArrayList<>();

        long lastTimestamp = 0;
        long counter = 0;

        try
        {
            while (true)
            {
                long timestamp = lastTimestamp + 1000 + (r.nextInt(100) - 50);
                double value = r.nextDouble() * 1000;

                b.append(timestamp, value);
                counter++;

                lastTimestamp = timestamp;

                entries.add(new Entry(timestamp, value));
            }
        }
        catch (IndexOutOfBoundsException e)
        {
            // Stop
        }

        assertTimestampsAndValues(b, entries);
        System.out.println(counter);
    }

    @Test
    public void shouldValidateWithinRange() throws Exception
    {
        assertThat(Block.valueInRange(2, 1)).isTrue();
        assertThat(Block.valueInRange(63, 9196438390897981421L)).isTrue();
        assertThat(Block.valueInRange(2, 4)).isFalse();
    }

    private void assertTimestampsAndValues(Block b, List<Entry> entries)
    {
        Iterator<Entry> iterator = entries.iterator();

        b.foreach((t, v) -> {

            Entry entry = iterator.next();

            assertThat(t).isEqualTo(entry.timestamp);
            assertThat(v).isEqualTo(entry.value);
        });
    }

    private void assertWriteAndReadValues(long[] timestamps, double[] values)
    {
        for (int i = 0; i < timestamps.length; i++)
        {
            b.append(timestamps[i], values[i]);
        }

        int index[] = { 0 };

        b.foreach((t, v) -> {
            assertThat(t).as("Timestamp index: %d", index[0]).isEqualTo(timestamps[index[0]]);
            assertThat(v).as("Value index: %d", index[0]).isEqualTo(values[index[0]]);

            index[0]++;
        });

        assertThat(index[0]).isEqualTo(timestamps.length);
    }

    private static class Entry
    {
        private final long timestamp;
        private final double value;

        private Entry(long timestamp, double value)
        {
            this.timestamp = timestamp;
            this.value = value;
        }
    }
}
