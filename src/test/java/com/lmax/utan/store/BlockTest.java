package com.lmax.utan.store;

import org.agrona.concurrent.UnsafeBuffer;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static java.lang.Integer.toBinaryString;
import static org.assertj.core.api.Assertions.assertThat;

public class BlockTest
{
    private final Block b = new Block(new UnsafeBuffer(new byte[4096]));
    private final Block copy = new Block(new UnsafeBuffer(new byte[4096]));

    @Test
    public void iterateOnEmptyBlock() throws Exception
    {
        final int[] c = { 0 };
        b.foreach((t, v) -> {
            c[0]++;
        });

        assertThat(c[0]).isEqualTo(0);
    }

    @Test
    public void storeAndGetFirstValue() throws Exception
    {
        long timestamp = System.currentTimeMillis();
        double val = 78432.3;

        long[] timestamps = { timestamp };
        double[] values = { val };

        assertWriteAndReadValues(timestamps, values);
    }

    @Test
    public void storeAndGetSubsequentDoubleDeltaOfZero() throws Exception
    {
        long timestamp = System.currentTimeMillis();
        double val = 78432.3;

        long[] timestamps = { timestamp, timestamp };
        double[] values = { val, val };

        assertWriteAndReadValues(timestamps, values);
    }

    @Test
    public void storeAndGetSubsequentDoubleDeltaOfOneMillisecond() throws Exception
    {
        long timestamp = System.currentTimeMillis();
        double val = 78432.3;

        long[] timestamps = { timestamp, timestamp + 1000, timestamp + 2000 };
        double[] values = { val, val + 1, val - 1 };

        assertWriteAndReadValues(timestamps, values);
    }

    @Test
    public void compressTo7Bits()
    {
        for (int i = -64, j = 64; i < 0; i++, j++)
        {
            Assertions.assertThat(Block.compressBits(i, 7)).isEqualTo(j);
            Assertions.assertThat(Block.decompressBits(Block.compressBits(i, 7), 7)).isEqualTo(i);
        }

        for (int i = 0, j = 0; i < 64; i++, j++)
        {
            Assertions.assertThat(Block.compressBits(i, 7)).isEqualTo(j);
            Assertions.assertThat(Block.decompressBits(Block.compressBits(i, 7), 7)).isEqualTo(i);
        }
    }

    @Test
    public void zeros() throws Exception
    {
        assertThat(Long.numberOfLeadingZeros(0)).isEqualTo(64);
        assertThat(Long.numberOfTrailingZeros(0)).isEqualTo(64);
    }

    @Test
    public void shouldInsert500DataPoints() throws Exception
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

        long[] timestamps = { 0, Integer.MAX_VALUE };
        double[] values = { 0, d };

        assertWriteAndReadValues(timestamps, values);
    }

    @Test
    public void shouldWriteUntilBufferIsFull() throws Exception
    {
        Random r = new Random(7);

        List<Entry> entries = new ArrayList<>();

        long lastTimestamp = 0;
        do
        {
            long timestamp = lastTimestamp + 1000 + (r.nextInt(100) - 50);
            double value = r.nextDouble() * 1000;

            if(!b.append(timestamp, value))
            {
                break;
            }

            lastTimestamp = timestamp;
            entries.add(new Entry(timestamp, value));
        }
        while (true);

        assertTimestampsAndValues(b, entries);
    }

    @Test
    public void shouldValidateWithinRange() throws Exception
    {
        assertThat(Block.valueInRange(2, 1)).isTrue();
        assertThat(Block.valueInRange(63, 9196438390897981421L)).isTrue();
        assertThat(Block.valueInRange(2, 4)).isFalse();
    }

    @Test
    public void shouldAppendBitsToBuffer() throws Exception
    {
        b.writeBits(0, 0b10001L, 5);
        b.writeBits(5, 0b10001L, 5);
        b.writeBits(10, 0b10001L, 5);
        b.writeBits(15, 0b10001L, 5);
        b.writeBits(20, 0b11111111111111111111L, 20);
        b.writeBits(40, 0b1010101_01010101_01010101_01010101_01010101_01010101_01010101L, 55);
        b.writeBits(95, 0b111, 3);

        assertThat(toBinaryString(b.getTempPart(0))).isEqualTo("10001100011000110001111111111111");
        assertThat(toBinaryString(b.getTempPart(1))).isEqualTo("11111111101010101010101010101010");
        assertThat(toBinaryString(b.getTempPart(2))).isEqualTo("10101010101010101010101010101011");
        assertThat(toBinaryString(b.getTempPart(3))).isEqualTo("11000000000000000000000000000000");
    }

    private void assertWriteAndReadValues(long[] timestamps, double[] values)
    {
        final List<Entry> entries = new ArrayList<>(timestamps.length);

        for (int i = 0; i < timestamps.length; i++)
        {
            b.append(timestamps[i], values[i]);
            entries.add(new Entry(timestamps[i], values[i]));
        }

        b.copyTo(copy);

        assertThat(b.compareTo(copy)).isEqualTo(0);

        assertTimestampsAndValues(b, entries);
        assertTimestampsAndValues(copy, entries);
    }

    private void assertTimestampsAndValues(Block b, List<Entry> entries)
    {
        final Iterator<Entry> iterator = entries.iterator();
        final int index[] = { 0 };

        b.foreach((t, v) -> {
            final Entry entry = iterator.next();
            assertThat(t).isEqualTo(entry.timestamp);
            assertThat(v).isEqualTo(entry.value);

            index[0]++;
        });

        assertThat(index[0]).isEqualTo(entries.size());
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
