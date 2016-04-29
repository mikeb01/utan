package com.lmax.utan;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.lmax.utan.Block.compressBits;
import static com.lmax.utan.Block.decompressBits;
import static org.assertj.core.api.Assertions.assertThat;

public class BlockTest
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[1024]);
    private final Block b = new Block(buffer);

    @Test
    public void storeAndGetFirstValue() throws Exception
    {
        long timestamp = System.currentTimeMillis();
        double val = 78432.3;

        assertThat(b.lengthInBits()).isEqualTo(4 * 8);

        b.append(timestamp, val);

        assertThat(b.lengthInBits()).isEqualTo(20 * 8);

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

        assertThat(b.lengthInBits()).isEqualTo(4 * 8);

        b.append(timestamp, val);
        b.append(timestamp, val);

        assertThat(b.lengthInBits()).isEqualTo((20 * 8) + 2);

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

        assertThat(b.lengthInBits()).isEqualTo(4 * 8);

        b.append(timestamp, val);
        b.append(timestamp + 1000, val + 1);
        b.append(timestamp + 2000, val - 1);

//        assertThat(b.lengthInBits()).isEqualTo((20 * 8) + 2);

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
}
