package com.lmax.utan;

import com.lmax.utan.store.Block;
import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.Random;

@State(Scope.Benchmark)
public class BlockBenchmark
{
    private final long[] timestamps = new long[1024];
    private final double[] values = new double[1024];
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[4096]);
    private final Block block = new Block(buffer);

    @Setup
    public void setUp()
    {
        final Random r = new Random(3);
        long lastTimestamp = System.currentTimeMillis();

        for (int i = 0; i < timestamps.length; i++)
        {
            timestamps[i] = lastTimestamp;
            lastTimestamp += (r.nextInt(200) - 100);

            values[i] = f(r.nextDouble());
        }

    }

    @Setup(Level.Invocation)
    public void perInvocation()
    {
        buffer.setMemory(0, 4096, (byte) 0);
    }

    @Benchmark
    public void fillBlock()
    {
        try
        {
            for (int i = 0; ; i++)
            {
                long timestamp = timestamps[i % (1024 - 1)];
                double value = values[i % (1024 - 1)];

                block.append(timestamp, value);
            }
        }
        catch (Exception e)
        {
            // Ignore
        }
    }

    private static double f(double v)
    {
        if (v < 0.5)
        {
            return 0;
        }
        else if (v < 0.9)
        {
            return (v - 0.5) * 20;
        }
        else
        {
            return (v - 0.9) * 2000;
        }
    }
}
