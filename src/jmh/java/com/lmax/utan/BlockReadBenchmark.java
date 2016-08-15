package com.lmax.utan;

import com.lmax.utan.store.Block;
import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;

@State(Scope.Benchmark)
public class BlockReadBenchmark
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[4096]);
    private final Block block = new Block(buffer);
    private PerfConsumer consumer = new PerfConsumer();

    @Setup
    public void setUp()
    {
        final Random r = new Random(3);
        long lastTimestamp = System.currentTimeMillis();

        try
        {
            while (true)
            {
                block.append(lastTimestamp, f(r.nextDouble()));
            }
        }
        catch (Exception e)
        {
            // Ignore
        }
    }

    @Benchmark
    public void consumeBlock(Blackhole bh)
    {
        consumer.blackhole = bh;
        block.foreach(consumer);
    }

    private static class PerfConsumer implements Block.ValueConsumer
    {
        private Blackhole blackhole;

        @Override
        public void accept(long timestamp, double value)
        {
            blackhole.consume(timestamp);
            blackhole.consume(value);
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
