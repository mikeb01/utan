package com.lmax.utan.store;

import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class OffHeapMemoryTimeSeries
{
    private final Block[] blocks;
    private final int controlHeadOffset = 0;
    private final int controlTailForReadingOffset = 8;
    private final int controlTailForResettingOffset = 16;
    private final int maxActiveBlocks;
    private final FileChannel data;
    private final UnsafeBuffer controlBuffer;
    private final FileChannel control;
    private final MappedByteBuffer dataMMap;
    private final MappedByteBuffer controlMMap;

    public OffHeapMemoryTimeSeries(int numBlocks, Path controlFile, Path dataFile) throws IOException
    {
        if (Integer.bitCount(numBlocks) != 1)
        {
            throw new RuntimeException("numBlocks must be a power of 2");
        }
        else if (numBlocks < 4)
        {
            throw new RuntimeException("numBlocks must at least 4");
        }

        control = FileChannel.open(controlFile, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        data = FileChannel.open(dataFile, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

        dataMMap = data.map(FileChannel.MapMode.READ_WRITE, 0, numBlocks * Block.BYTE_LENGTH);
        controlMMap = control.map(FileChannel.MapMode.READ_WRITE, 0, 24);

        controlBuffer = new UnsafeBuffer(controlMMap, 0, 24);
        controlBuffer.setMemory(0, 24, (byte) 0);

        blocks = new Block[numBlocks];
        for (int i = 0; i < numBlocks; i++)
        {
            UnsafeBuffer blockBuffer = new UnsafeBuffer(dataMMap, i * Block.BYTE_LENGTH, Block.BYTE_LENGTH);
            blocks[i] = new Block(blockBuffer);
            blocks[i].reset();
        }

        this.maxActiveBlocks = (numBlocks / 4) * 3;
    }

    public static OffHeapMemoryTimeSeries create(int numBlocks, Path directory) throws IOException
    {
        Files.createDirectories(directory);
        Path dataFile = directory.resolve("data.bin");
        Path controlFile = directory.resolve("control.bin");

        Files.deleteIfExists(dataFile);
        Files.deleteIfExists(controlFile);

        return new OffHeapMemoryTimeSeries(numBlocks, controlFile, dataFile);
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
        long head = getHead() - 1;

        if (head < 0)
        {
            head = incrementHead() - 1;
        }

        if (!blocks[indexOf(head)].append(timestamp, value).isOk())
        {
            final long nextHead = head + 1;

            if (nextHead - getTailForReading() >= maxActiveBlocks)
            {
                incrementTailForReading();
            }

            if (nextHead - getTailForReseting() < blocks.length)
            {
                incrementHead();

                if (!blocks[indexOf(nextHead)].append(timestamp, value).isOk())
                {
                    throw new RuntimeException("WAT");
                }
            }
            else
            {
                throw new RuntimeException("No capacity");
            }
        }

        for (long l = getTailForReseting(), n = getTailForReading(); l < n; l++)
        {
            Block blockToReset = blocks[indexOf(l)];

            if (!blockToReset.reset())
            {
                break;
            }

            incrementTailForReseting();
        }
    }

    public void query(long startTimestamp, long endTimestamp, ValueConsumer consumer)
    {
        long head = getHead();
        long tail = getTailForReading();

        for (long i = tail; i < head; i++)
        {
            Block b = blocks[indexOf(i)];
            b.foreach(
                (k, v) ->
                {
                    if (startTimestamp <= k && k < endTimestamp)
                    {
                        return consumer.accept(k, v);
                    }

                    return k < endTimestamp;
                });
        }
    }

    public long getHead()
    {
        return controlBuffer.getLongVolatile(controlHeadOffset);
    }

    private long incrementHead()
    {
        return controlBuffer.getAndAddLong(controlHeadOffset, 1L) + 1L;
    }

    private long getTailForReading()
    {
        return controlBuffer.getLongVolatile(controlTailForReadingOffset);
    }

    private long incrementTailForReading()
    {
        return controlBuffer.getAndAddLong(controlTailForReadingOffset, 1L) + 1L;
    }

    private long getTailForReseting()
    {
        return controlBuffer.getLongVolatile(controlTailForResettingOffset);
    }

    private long incrementTailForReseting()
    {
        return controlBuffer.getAndAddLong(controlTailForResettingOffset, 1L) + 1L;
    }

    private int getAndIncrementHeadIndex()
    {
        return indexOf(controlBuffer.getAndAddLong(controlHeadOffset, 1L));
    }

    private int indexOf(long i)
    {
        return (int) (i & (blocks.length - 1));
    }

    public int capacity()
    {
        return blocks.length;
    }
}
