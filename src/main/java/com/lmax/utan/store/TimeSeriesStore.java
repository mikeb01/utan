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
        long day = timestamp / (24 * 60 * 60);

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
