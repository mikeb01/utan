package com.lmax.utan.store;

import java.util.concurrent.atomic.AtomicInteger;

public class ValueDataSeries
{
    private final int id;
    private final int numBlocks;
    private final Block[] blocks;
    private final AtomicInteger headBlock = new AtomicInteger(0);
    private final AtomicInteger tailBlock = new AtomicInteger(0);

    public ValueDataSeries(int id, int numBlocks)
    {
        this.id = id;
        this.numBlocks = numBlocks;
        this.blocks = new Block[numBlocks];
    }


    public void load(BlockSource blockSource)
    {
        for (Block block : blockSource.lastN(blocks.length))
        {
            int index = headBlock.getAndIncrement();
            blocks[index] = block;
        }
    }


    public void query(long startTimestamp, long endTimestamp, Block.ValueConsumer consumer)
    {
        int head = headBlock.get();
        int tail = tailBlock.get();

        for (int i = tail; i < head; i++)
        {
            Block b = blocks[i];
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
}
