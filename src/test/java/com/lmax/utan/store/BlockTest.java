package com.lmax.utan.store;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static com.lmax.utan.store.Block.AppendStatus.FROZEN;
import static java.lang.Integer.toBinaryString;
import static org.assertj.core.api.Assertions.assertThat;

public class BlockTest
{
    private final Block b = Block.newHeapBlock();
    private final Block copy = Block.newHeapBlock();

    @Test
    public void iterateOnEmptyBlock() throws Exception
    {
        final int[] c = {0};
        b.foreach((t, v) -> {
            c[0]++;
            return true;
        });

        assertThat(c[0]).isEqualTo(0);
        assertThat(b.isEmpty()).isTrue();
        assertThat(b.firstTimestamp()).isZero();
    }

    @Test
    public void storeAndGetFirstValue() throws Exception
    {
        long timestamp = System.currentTimeMillis();
        double val = 78432.3;

        long[] timestamps = {timestamp};
        double[] values = {val};

        assertWriteAndReadValues(timestamps, values);
        assertThat(b.lengthInBits()).isEqualTo(Block.COMPRESSED_DATA_START_BITS);
        assertThat(b.firstTimestamp()).isEqualTo(timestamp);
    }

    @Test
    public void storeAndGetSubsequentDoubleDeltaOfZero() throws Exception
    {
        long timestamp = System.currentTimeMillis();
        double val = 78432.3;

        long[] timestamps = {timestamp, timestamp};
        double[] values = {val, val};

        assertWriteAndReadValues(timestamps, values);
    }

    @Test
    public void storeAndGetSubsequentDoubleDeltaOfOneMillisecond() throws Exception
    {
        long timestamp = System.currentTimeMillis();
        double val = 78432.3;

        long[] timestamps = {timestamp, timestamp + 1000, timestamp + 2000};
        double[] values = {val, val + 1, val - 1};

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

        long[] timestamps = {0, Integer.MAX_VALUE};
        double[] values = {0, d};

        assertWriteAndReadValues(timestamps, values);
    }

    @Test
    public void shouldHandleValuesThatCreateLeadingZerosOf32() throws Exception
    {
        long[] timestamps = { 1501922076174L, 1501922106174L, 1501922136174L, 1501922166174L, 1501922196174L, };
        double[] values = { 4968952.000000, 4968952.000000, 4968952.000000, 4968952.000000, 4968953.000000 }; /* 4968958.000000 */

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

            if (!b.append(timestamp, value).isOk())
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

    @Test
    public void freezeBlockWithChecksum() throws Exception
    {
        TimeSeriesSupplier supplier = new TimeSeriesSupplier(11111);

        for (int i = 0; i < 100; i++)
        {
            Entry entry = supplier.get();
            b.append(entry.timestamp, entry.value);
        }

        b.freeze();

        Entry entry = supplier.get();
        assertThat(b.append(entry.timestamp, entry.value)).isEqualTo(FROZEN);
        assertThat(b.isFrozen()).isTrue();
    }

    @Test
    public void shouldAllowEarlyExitFromConsumer() throws Exception
    {
        TimeSeriesSupplier supplier = new TimeSeriesSupplier(11111);

        for (int i = 0; i < 100; i++)
        {
            Entry entry = supplier.get();
            b.append(entry.timestamp, entry.value);
        }

        int[] count = {0};
        assertThat(b.foreach((k, v) -> ++count[0] < 5)).isEqualTo(5);
        assertThat(count[0]).isEqualTo(5);

        count[0] = 0;
        assertThat(b.foreach((k, v) -> ++count[0] < 1)).isEqualTo(1);
        assertThat(count[0]).isEqualTo(1);
    }

    private void assertWriteAndReadValues(long[] timestamps, double[] values)
    {
        final List<Entry> entries = new ArrayList<>(timestamps.length);

        int numWritten = 0;
        for (; numWritten < timestamps.length; )
        {
            if (b.append(timestamps[numWritten], values[numWritten]).isOk())
            {
                entries.add(new Entry(timestamps[numWritten], values[numWritten]));
                numWritten++;
            }
            else
            {
                break;
            }
        }

        b.copyTo(copy);

        assertThat(copy).isEqualTo(b);
        assertThat(b.compareTo(copy)).isEqualTo(0);

        assertTimestampsAndValues(b, entries.subList(0, numWritten));
        assertTimestampsAndValues(copy, entries.subList(0, numWritten));
    }

    private void assertTimestampsAndValues(Block b, List<Entry> entries)
    {
        final Iterator<Entry> iterator = entries.iterator();
        final int index[] = {0};

        int count = b.foreach((t, v) -> {
            final Entry entry = iterator.next();
            assertThat(t).isEqualTo(entry.timestamp);
            assertThat(v).isEqualTo(entry.value);

            index[0]++;
            return true;
        });

        assertThat(index[0]).isEqualTo(entries.size());
        assertThat(count).isEqualTo(entries.size());
    }

}
