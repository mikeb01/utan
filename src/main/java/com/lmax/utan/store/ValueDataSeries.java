package com.lmax.utan.store;

import java.util.concurrent.atomic.AtomicLong;

public class ValueDataSeries
{
    private final int id;
    private final int numBlocks;
    private final Block[] blocks;
    private final AtomicLong headBlock = new AtomicLong(0);
    private final AtomicLong tailForReseting = new AtomicLong(0);
    private final AtomicLong tailForReading = new AtomicLong(0);
    private final int maxActiveBlocks;

    public ValueDataSeries(int id, int numBlocks)
    {
        if (Integer.bitCount(numBlocks) != 1)
        {
            throw new RuntimeException("numBlocks must be a power of 2");
        }
        else if (numBlocks < 4)
        {
            throw new RuntimeException("numBlocks must at least 4");
        }

        this.id = id;
        this.numBlocks = numBlocks;
        this.blocks = Block.new4KDirectBlocks(numBlocks);
        this.maxActiveBlocks = (numBlocks / 4) * 3;
    }

    public synchronized void load(BlockSource blockSource)
    {
        for (Block block : blockSource.lastN(blocks.length))
        {
            int index = getAndIncrementHeadIndex();
            block.copyTo(blocks[index]);
        }
    }

    public synchronized void append(long timestamp, double value)
    {
        long head = headBlock.get() - 1;

        if (head < 0)
        {
            head = headBlock.incrementAndGet() - 1;
        }

        if (!blocks[indexOf(head)].append(timestamp, value))
        {
            final long nextHead = head + 1;

            if (nextHead - tailForReading.get() >= maxActiveBlocks)
            {
                tailForReading.incrementAndGet();
            }

            if (nextHead - tailForReseting.get() < blocks.length)
            {
                headBlock.incrementAndGet();

                if (!blocks[indexOf(nextHead)].append(timestamp, value))
                {
                    throw new RuntimeException("WAT");
                }
            }
            else
            {
                throw new RuntimeException("No capacity");
            }
        }

        for (long l = tailForReseting.get(), n = tailForReading.get(); l < n; l++)
        {
            Block blockToReset = blocks[indexOf(l)];

            if (!blockToReset.reset())
            {
                break;
            }

            tailForReseting.incrementAndGet();
        }
    }


    public void query(long startTimestamp, long endTimestamp, Block.ValueConsumer consumer)
    {
        long head = headBlock.get();
        long tail = tailForReading.get();

        for (long i = tail; i < head; i++)
        {
            Block b = blocks[indexOf(i)];
            b.foreach(
                (k, v) ->
                {
                    if (startTimestamp <= k && k < endTimestamp)
                    {
                        consumer.accept(k, v);
                    }
                });
        }
    }

    public long head()
    {
        return headBlock.get();
    }

    private int getAndIncrementHeadIndex()
    {
        return indexOf(headBlock.getAndIncrement());
    }

    private int indexOf(long andIncrement)
    {
        return (int) (andIncrement & (blocks.length - 1));
    }

    public int capacity()
    {
        return blocks.length;
    }
}
