package com.lmax.utan;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

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

        assertThat(b.lenghtInBits()).isEqualTo(4 * 8);

        b.append(timestamp, val);

        assertThat(b.lenghtInBits()).isEqualTo(20 * 8);

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

        assertThat(b.lenghtInBits()).isEqualTo(4 * 8);

        b.append(timestamp, val);
        b.append(timestamp, val);

        assertThat(b.lenghtInBits()).isEqualTo((20 * 8) + 2);

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
}
