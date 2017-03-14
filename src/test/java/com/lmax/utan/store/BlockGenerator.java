package com.lmax.utan.store;

import java.util.List;
import java.util.Random;

public class BlockGenerator
{
    public static long generateBlockData(final long startTimestamp, Random r, Block block, List<Entry> entries)
    {
        long timestamp = startTimestamp;
        int value;

        do
        {
            value = r.nextInt(101);

            if (!block.append(timestamp, value))
            {
                break;
            }

            entries.add(new Entry(timestamp, value));

            timestamp += (1000 + (50 - r.nextInt(100)));
        }
        while (true);

        return timestamp;
    }
}
