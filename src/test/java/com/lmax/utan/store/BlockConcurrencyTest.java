package com.lmax.utan.store;

import org.junit.Before;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

public class BlockConcurrencyTest
{
    private final long[] timestamps = new long[1024];
    private final double[] values = new double[1024];
    private final Random r = new Random(87324);
    private final Block block = Block.newHeapBlock();

    @Before
    public void setUp()
    {
        long lastTimestamp = System.currentTimeMillis();

        for (int i = 0; i < timestamps.length; i++)
        {
            timestamps[i] = lastTimestamp;
            lastTimestamp += (r.nextInt(200) - 100);

            values[i] = f(r.nextDouble());
        }
    }

    @Test
    public void ensureConcurrentReadsSeeAConsistentView() throws Exception
    {
        Verifier verifier = new Verifier();
        CountDownLatch latch = new CountDownLatch(1);
        Thread t = new Thread(() ->
        {
            latch.countDown();

            while (!Thread.currentThread().isInterrupted())
            {
                for (int i = 0; i < timestamps.length; i++)
                {
                    if (!block.append(timestamps[i], values[i]).isOk())
                    {
                        break;
                    }
                }

                while (!block.reset())
                {
                    // No-op
                }
            }
        });

        t.setDaemon(true);
        t.start();
        latch.await();

        long t0 = System.currentTimeMillis();

        long total = 0;

        while (System.currentTimeMillis() - t0 < 5000)
        {
            for (int i = 0; i < 20_000; i++)
            {
                block.foreach(verifier);
                total += verifier.counter;
                verifier.reset();
            }
        }

        assertThat(total).isNotEqualTo(0);
    }

    @Test
    public void ensureConcurrentCopiesSeeAConsistentView() throws Exception
    {
        Verifier verifier = new Verifier();
        CountDownLatch latch = new CountDownLatch(1);
        Thread t = new Thread(() ->
        {
            latch.countDown();

            while (!Thread.currentThread().isInterrupted())
            {
                for (int i = 0; i < timestamps.length; i++)
                {
                    if (!block.append(timestamps[i], values[i]).isOk())
                    {
                        break;
                    }
                }

                block.reset();
            }
        });

        t.setDaemon(true);
        t.start();
        latch.await();

        final long t0 = System.currentTimeMillis();
        final Block copy = new Block();
        long total = 0;

        while (System.currentTimeMillis() - t0 < 5000)
        {
            for (int i = 0; i < 20_000; i++)
            {
                block.copyTo(copy);
                copy.foreach(verifier);
                total += verifier.counter;
                verifier.reset();
                copy.reset();
            }
        }

        assertThat(total).isNotEqualTo(0);
    }

    private class Verifier implements ValueConsumer
    {
        private int counter = 0;

        @Override
        public void accept(long timestamp, double value)
        {
            assertThat(timestamp).isEqualTo(timestamps[counter]);
            assertThat(value).isEqualTo(values[counter]);

            counter++;
        }

        public void reset()
        {
            counter = 0;
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
