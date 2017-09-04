package com.lmax.utan.store;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TimeSeriesStore
{
    private final Map<String, CurrentBlock> nameToBlockMap = new HashMap<>();
    private final PersistentStoreWriter writer;

    public TimeSeriesStore(PersistentStoreWriter writer)
    {
        this.writer = writer;
    }

    public void store(String name, long timestamp, double value) throws IOException
    {
        long day = getDay(timestamp);

        final CurrentBlock currentBlock = nameToBlockMap.computeIfAbsent(name, s -> new CurrentBlock(Block.newDirectBlock(), day));

        if (currentBlock.currentDay != day)
        {
            currentBlock.block.freeze();
            writer.store(name, currentBlock.block);
            currentBlock.reset(day);
        }

        if (!currentBlock.block.append(timestamp, value).isOk())
        {
            currentBlock.block.freeze();
            writer.store(name, currentBlock.block);
            currentBlock.reset(day);

            if (!currentBlock.block.append(timestamp, value).isOk())
            {
                throw new RuntimeException("WAT");
            }
        }
    }

    public void flush(String name) throws IOException
    {
        final CurrentBlock currentBlock = nameToBlockMap.get(name);
        if (null != currentBlock)
        {
            currentBlock.block.freeze();
            writer.store(name, currentBlock.block);
        }
    }

    static long getDay(final long timestamp)
    {
        return timestamp / (24 * 60 * 60 * 1000);
    }

    private static class CurrentBlock
    {
        private Block block;
        private long currentDay;

        private CurrentBlock(Block block, long currentDay)
        {
            this.block = block;
            this.currentDay = currentDay;
        }

        public void reset(long day)
        {
            block.reset();
            currentDay = day;
        }
    }
}
