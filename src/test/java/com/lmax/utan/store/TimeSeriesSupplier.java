package com.lmax.utan.store;

import java.util.Random;
import java.util.function.Supplier;

import static java.lang.Math.abs;

public class TimeSeriesSupplier implements Supplier<Entry>
{
    private final Random r;
    private long lastTimestamp = -1;
    private final long beginTimestamp;

    public TimeSeriesSupplier(long seed)
    {
        this(seed, -1);
    }

    public TimeSeriesSupplier(long seed, long startTimestamp)
    {
        r = new Random(seed);
        beginTimestamp = startTimestamp;
    }

    @Override
    public Entry get()
    {
        if (lastTimestamp == -1)
        {
            lastTimestamp = beginTimestamp == -1 ? abs(r.nextLong()) : beginTimestamp;
        }
        else
        {
            lastTimestamp += (1000 + (50 - r.nextInt(100)));
        }

        double value = r.nextDouble() * 100;

        return new Entry(lastTimestamp, value);
    }
}
